"""
Streamlit application for analyzing dbt freshness configuration.

Usage:
    streamlit run streamlit_freshness_app.py
"""

import streamlit as st
import pandas as pd
import requests
from log_freshness import DBTFreshnessLogger
import json
import ast
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List
import plotly.express as px
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, as_completed


# dbt Cloud run status codes
RUN_STATUS_CODES = {
    1: 'queued',
    2: 'starting',
    3: 'running',
    10: 'success',
    20: 'error',
    30: 'cancelled',
}

# Common packages to exclude (not part of main project)
EXCLUDED_PACKAGES = [
    'dbt_project_evaluator',
    'dbt_artifacts',
    'dbt_utils',
    'dbt_expectations',
    'codegen',
    'audit_helper',
    'dbt_meta_testing',
    'elementary',
    're_data'
]


def filter_to_main_project(df: pd.DataFrame, package_column: str = 'packageName') -> pd.DataFrame:
    """
    Filter dataframe to only include models from the main project.
    Excludes common dbt packages that users don't control.
    
    Args:
        df: DataFrame with package information
        package_column: Name of the column containing package names
    
    Returns:
        Filtered DataFrame with only main project models
    """
    if package_column not in df.columns:
        return df
    
    # Filter out excluded packages
    mask = ~df[package_column].isin(EXCLUDED_PACKAGES)
    filtered_df = df[mask].copy()
    
    return filtered_df


def determine_job_type(triggers: dict) -> str:
    """
    Determine job type from triggers.
    
    Args:
        triggers: Job triggers dictionary from dbt Cloud API
    
    Returns:
        'ci', 'merge', 'scheduled', or 'other'
    """
    # Has schedule trigger
    if triggers.get('schedule'):
        return 'scheduled'
    
    # Has webhook trigger
    if triggers.get('github_webhook') or triggers.get('on_merge'):
        # Check if custom branch only (CI) or not (merge)
        if triggers.get('custom_branch_only', False):
            return 'ci'
        else:
            return 'merge'
    
    # Everything else
    return 'other'


def filter_jobs_by_type(jobs: list, job_types: list) -> list:
    """
    Filter jobs by their type (ci, merge, scheduled, other).
    
    Args:
        jobs: List of job dictionaries from dbt Cloud API
        job_types: List of job types to include ('ci', 'merge', 'scheduled', 'other')
    
    Returns:
        Filtered list of jobs
    """
    filtered = []
    
    for job in jobs:
        triggers = job.get('triggers', {})
        job_type = determine_job_type(triggers)
        
        if job_type in job_types:
            filtered.append(job)
    
    return filtered


def check_job_has_sao(job_data: dict) -> bool:
    """
    Check if a job has State-Aware Orchestration (SAO) enabled.
    
    Args:
        job_data: Job dictionary from dbt Cloud API (from run's 'job' related object)
    
    Returns:
        True if SAO is enabled, False otherwise
    """
    if not job_data:
        return False
    
    cost_optimization_features = job_data.get('cost_optimization_features', [])
    return 'state_aware_orchestration' in cost_optimization_features


def filter_runs_by_sao(runs: list) -> tuple:
    """
    Filter runs to only include those from jobs with SAO enabled.
    
    Args:
        runs: List of run dictionaries from dbt Cloud API (with 'job' in include_related)
    
    Returns:
        Tuple of (sao_runs, non_sao_runs)
    """
    sao_runs = []
    non_sao_runs = []
    
    for run in runs:
        job_data = run.get('job')
        if check_job_has_sao(job_data):
            sao_runs.append(run)
        else:
            non_sao_runs.append(run)
    
    return sao_runs, non_sao_runs


def get_status_name(status):
    """Convert status code to readable name."""
    if isinstance(status, int):
        return RUN_STATUS_CODES.get(status, f'unknown({status})')
    return status


def get_job_runs(api_base: str, api_key: str, account_id: str, job_id: str, 
                 environment_id: str = None, limit: int = 20, status: List[int] = None):
    """
    Fetch recent runs for a specific job.
    
    Args:
        api_base: dbt Cloud API base URL
        api_key: dbt Cloud API key
        account_id: dbt Cloud account ID
        job_id: Job definition ID
        environment_id: Optional environment ID (Note: not used when querying by job_id as it's redundant)
        limit: Max number of runs to fetch (per status if multiple). Will paginate if > 100.
        status: Optional list of status codes to filter by (10=success, 20=error, 30=cancelled)
                Note: API accepts one status per request, so we make multiple calls if needed
    
    Returns:
        List of run objects
    """
    url = f'{api_base}/api/v2/accounts/{account_id}/runs/'
    headers = {'Authorization': f'Token {api_key}'}
    
    # API has a max limit of 100 per request
    API_MAX_LIMIT = 100
    
    # If no status filter or only one status, make API call(s) with pagination if needed
    if not status or len(status) == 1:
        all_runs = []
        runs_to_fetch = limit
        offset = 0
        
        # Paginate if limit exceeds API max
        while runs_to_fetch > 0:
            page_limit = min(runs_to_fetch, API_MAX_LIMIT)
            
            params = {
                'limit': page_limit,
                'offset': offset,
                'order_by': '-id',
                'job_definition_id': job_id,
                'include_related': '["job","trigger","environment","repository"]',
            }
            
            if status:
                params['status'] = status[0]
            
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            page_runs = data.get('data', [])
            
            if not page_runs:
                break  # No more runs available
            
            all_runs.extend(page_runs)
            
            # If we got fewer runs than requested, we've reached the end
            if len(page_runs) < page_limit:
                break
            
            runs_to_fetch -= len(page_runs)
            offset += len(page_runs)
        
        return all_runs
    
    # Multiple statuses: make separate API calls (with pagination) and combine results
    all_runs = []
    seen_run_ids = set()
    errors = []
    
    for status_code in status:
        runs_to_fetch = limit
        offset = 0
        
        # Paginate if limit exceeds API max
        while runs_to_fetch > 0:
            page_limit = min(runs_to_fetch, API_MAX_LIMIT)
            
            params = {
                'limit': page_limit,
                'offset': offset,
                'order_by': '-id',
                'job_definition_id': job_id,
                'include_related': '["job","trigger","environment","repository"]',
                'status': status_code
            }
            
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                
                data = response.json()
                page_runs = data.get('data', [])
                
                if not page_runs:
                    break  # No more runs available for this status
                
                # Avoid duplicates (shouldn't happen, but just in case)
                for run in page_runs:
                    run_id = run.get('id')
                    if run_id not in seen_run_ids:
                        seen_run_ids.add(run_id)
                        all_runs.append(run)
                
                # If we got fewer runs than requested, we've reached the end
                if len(page_runs) < page_limit:
                    break
                
                runs_to_fetch -= len(page_runs)
                offset += len(page_runs)
                
            except requests.exceptions.HTTPError as e:
                status_name = {10: 'Success', 20: 'Error', 30: 'Cancelled'}.get(status_code, str(status_code))
                # Print the full URL for debugging
                print(f"⚠️ Warning: Failed to fetch {status_name} runs")
                print(f"   URL: {response.url if 'response' in locals() else 'N/A'}")
                print(f"   Error: {str(e)}")
                if hasattr(e.response, 'text'):
                    print(f"   Response: {e.response.text[:500]}")
                error_msg = f"Failed to fetch {status_name} runs (status code {status_code})"
                errors.append(error_msg)
                break  # Stop paginating this status on error
            except Exception as e:
                status_name = {10: 'Success', 20: 'Error', 30: 'Cancelled'}.get(status_code, str(status_code))
                error_msg = f"Failed to fetch {status_name} runs: {str(e)}"
                errors.append(error_msg)
                print(f"Warning: {error_msg}")
                break  # Stop paginating this status on error
    
    # If we got errors but no runs, raise the first error
    if errors and not all_runs:
        raise Exception(f"Failed to fetch runs. Errors: {'; '.join(errors)}")
    
    # If we got some runs but had errors, continue with what we got
    # (errors will be visible in console but won't break the analysis)
    
    # Sort by ID descending (most recent first)
    all_runs.sort(key=lambda x: x.get('id', 0), reverse=True)
    
    # Limit to requested number
    return all_runs[:limit]


def analyze_run_statuses(api_base: str, api_key: str, account_id: str, job_id: str, 
                         start_date: datetime = None, end_date: datetime = None, limit: int = 100):
    """
    Analyze run statuses for a job over a date range.
    
    Returns a dataframe with run status information.
    """
    # Fetch runs
    runs = get_job_runs(api_base, api_key, account_id, job_id, environment_id=None, limit=limit)
    
    # Filter by date if provided
    if start_date or end_date:
        filtered_runs = []
        for run in runs:
            created_at_str = run.get('created_at')
            if created_at_str:
                try:
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    # Remove timezone for comparison
                    created_at = created_at.replace(tzinfo=None)
                    
                    if start_date and created_at < start_date:
                        continue
                    if end_date and created_at > end_date:
                        continue
                    
                    filtered_runs.append(run)
                except:
                    pass
        runs = filtered_runs
    
    # Analyze each run
    all_model_statuses = []
    
    for run in runs:
        run_id = run.get('id')
        run_created = run.get('created_at')
        
        # Fetch run status data
        logger = DBTFreshnessLogger(api_base, api_key, account_id, str(run_id))
        try:
            result = logger.process_and_log(output_format='json', write_to_db=False, include_run_statuses=True)
            
            if result and isinstance(result, dict) and 'run_status_data' in result:
                run_status_data = result['run_status_data']
                
                if run_status_data and 'models' in run_status_data:
                    for model in run_status_data['models']:
                        model['run_id'] = run_id
                        model['run_created_at'] = run_created
                        all_model_statuses.append(model)
        except Exception as e:
            st.warning(f"Could not process run {run_id}: {str(e)}")
            continue
    
    if all_model_statuses:
        return pd.DataFrame(all_model_statuses)
    return pd.DataFrame()


def calculate_summary_stats(results):
    """Calculate summary statistics for freshness usage."""
    if not results:
        return None
    
    df = pd.DataFrame(results)
    
    # Extract package from unique_id if not already present
    if 'package' not in df.columns and 'unique_id' in df.columns:
        df['package'] = df['unique_id'].apply(lambda x: x.split('.')[1] if isinstance(x, str) and len(x.split('.')) > 1 else 'unknown')
    
    # Overall stats
    total_items = len(df)
    items_with_freshness = len(df[df['is_freshness_configured'] == True])
    items_without_freshness = total_items - items_with_freshness
    
    # By resource type
    resource_stats = []
    for resource_type in sorted(df['resource_type'].unique()):
        subset = df[df['resource_type'] == resource_type]
        total = len(subset)
        with_freshness = len(subset[subset['is_freshness_configured'] == True])
        pct = (with_freshness / total * 100) if total > 0 else 0
        
        resource_stats.append({
            'Resource Type': resource_type,
            'Total Count': total,
            'With Freshness': with_freshness,
            'Without Freshness': total - with_freshness,
            '% With Freshness': f'{pct:.1f}%'
        })
    
    # By package and resource type
    package_resource_stats = []
    if 'package' in df.columns:
        # Sort packages by count (descending) to show main project first
        package_counts = df['package'].value_counts()
        for package in package_counts.index:
            for resource_type in sorted(df['resource_type'].unique()):
                subset = df[(df['package'] == package) & (df['resource_type'] == resource_type)]
                if len(subset) > 0:  # Only include if there are items
                    total = len(subset)
                    with_freshness = len(subset[subset['is_freshness_configured'] == True])
                    pct = (with_freshness / total * 100) if total > 0 else 0
                    
                    package_resource_stats.append({
                        'Package': package,
                        'Resource Type': resource_type,
                        'Total Count': total,
                        'With Freshness': with_freshness,
                        'Without Freshness': total - with_freshness,
                        '% With Freshness': f'{pct:.1f}%'
                    })
    
    summary = {
        'overall': {
            'total': total_items,
            'with_freshness': items_with_freshness,
            'without_freshness': items_without_freshness,
            'pct_with_freshness': (items_with_freshness / total_items * 100) if total_items > 0 else 0
        },
        'by_resource': pd.DataFrame(resource_stats),
        'by_package_resource': pd.DataFrame(package_resource_stats) if package_resource_stats else None
    }
    
    return summary


def main():
    st.set_page_config(
        page_title="dbt Freshness & Run Analyzer",
        page_icon="🔍",
        layout="wide"
    )
    
    # Initialize session state for configuration
    if 'config' not in st.session_state:
        st.session_state.config = {
            'api_base': 'https://cloud.getdbt.com',
            'api_key': '',
            'account_id': '',
            'project_id': '',
            'environment_id': '',
            'configured': False
        }
    
    st.title("🔍 dbt Freshness & Run Status Analyzer")
    
    # Show configuration sidebar
    show_configuration_sidebar()
    
    # Create tabs for different pages
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "⚙️ Configuration",
        "🎯 Environment Overview", 
        "📋 Model Details", 
        "📈 Historical Trends",
        "💰 Cost Analysis",
        "🔀 Job Overlap Analysis"
    ])
    
    with tab1:
        show_configuration_page()
    
    with tab2:
        show_model_reuse_slo_analysis()
    
    with tab3:
        show_freshness_analysis()
    
    with tab4:
        show_run_status_analysis()
    
    with tab5:
        show_cost_analysis()
    
    with tab6:
        show_job_overlap_analysis()


def show_configuration_sidebar():
    """Show minimal configuration status in sidebar."""
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        if st.session_state.config['configured']:
            st.success("✅ Configured")
            st.caption(f"Account: {st.session_state.config['account_id']}")
            if st.session_state.config.get('environment_id'):
                st.caption(f"Environment: {st.session_state.config['environment_id']}")
            
            if st.button("🔄 Reconfigure", width='stretch'):
                st.session_state.config['configured'] = False
                st.rerun()
        else:
            st.warning("⚠️ Not Configured")
            st.caption("Go to Configuration tab to set up")


def show_configuration_page():
    """Show configuration page for setting up credentials and common settings."""
    st.header("⚙️ Configuration")
    st.markdown("Set up your dbt Cloud credentials and environment settings. These will be used across all analysis pages.")
    
    st.divider()
    
    # Configuration first (at the top)
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔐 dbt Cloud Credentials")
        
        api_base = st.text_input(
            "dbt Cloud URL",
            value=st.session_state.config['api_base'],
            help="Base URL for dbt Cloud instance (e.g., https://cloud.getdbt.com or https://vu491.us1.dbt.com - no trailing slash)",
            key="config_api_base"
        )
        
        api_key = st.text_input(
            "API Key",
            value=st.session_state.config['api_key'],
            type="password",
            help="dbt Cloud API token (keep this secret!)",
            key="config_api_key"
        )
        
        account_id = st.text_input(
            "Account ID",
            value=st.session_state.config['account_id'],
            help="Your dbt Cloud account ID",
            key="config_account_id"
        )
    
    with col2:
        st.subheader("🎯 Environment Settings")
        
        environment_id = st.text_input(
            "Environment ID (Optional)",
            value=st.session_state.config.get('environment_id', ''),
            help="dbt Cloud environment ID for environment-wide analysis",
            key="config_environment_id"
        )
        
        project_id = st.text_input(
            "Project ID (Optional)",
            value=st.session_state.config.get('project_id', ''),
            help="dbt Cloud project ID for filtering",
            key="config_project_id"
        )
    
    st.divider()
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("💾 Save Configuration", type="primary", width='stretch'):
            if not api_key or not account_id:
                st.error("❌ API Key and Account ID are required")
            else:
                st.session_state.config = {
                    'api_base': api_base.rstrip('/'),  # Strip trailing slash to avoid double slashes in URLs
                    'api_key': api_key,
                    'account_id': account_id,
                    'project_id': project_id,
                    'environment_id': environment_id,
                    'configured': True
                }
                st.success("✅ Configuration saved!")
                st.rerun()
    
    with col2:
        if st.button("🔄 Clear Configuration", width='stretch'):
            st.session_state.config = {
                'api_base': 'https://cloud.getdbt.com',
                'api_key': '',
                'account_id': '',
                'project_id': '',
                'environment_id': '',
                'configured': False
            }
            st.success("✅ Configuration cleared!")
            st.rerun()
    
    # Show current status
    if st.session_state.config['configured']:
        st.success("✅ Configuration is saved and ready to use")
        st.info("💡 **Tip**: Environment ID is used for Environment Overview and Job Overlap Analysis. Other tabs let you specify job/run details.")
    else:
        st.warning("⚠️ **Please configure your credentials to start analyzing**")
        st.info("ℹ️ Fill in the required fields and click 'Save Configuration' to get started")

    # Tab descriptions at the bottom
    st.divider()
    st.subheader("📖 About the Analysis Tabs")
    
    with st.expander("🎯 Environment Overview - Main Dashboard"):
        st.markdown("""
        **Your primary source of truth** for environment-wide health and performance.
        - Real-time status from dbt Cloud GraphQL API
        - Key Metrics: Reuse rate, freshness coverage, SLO compliance
        - Automatic Insights and recommendations
        
        **Requires**: Environment ID
        """)
    
    with st.expander("📋 Model Details - Configuration Deep Dive"):
        st.markdown("""
        **Deep dive into individual model configurations** from job manifest.
        - Model-by-model freshness configuration
        - Filter by job type (ci, merge, scheduled, other)
        - Export detailed reports
        
        **Flexible**: Use latest run from environment or specify job ID
        """)
    
    with st.expander("📈 Historical Trends - Performance Over Time"):
        st.markdown("""
        **Analyze performance patterns** across multiple job runs.
        - ⚡ Parallel processing for fast analysis
        - Reuse rate trending over time
        - Filter by job and job type
        
        **Flexible**: Analyze all runs in environment or filter to specific jobs
        """)
    
    with st.expander("💰 Cost Analysis - Financial Impact"):
        st.markdown("""
        **Quantify the financial impact** of your optimization efforts.
        - Cost estimation based on warehouse size
        - Savings from model reuse
        - ROI analysis
        
        **Requires**: Job runs to analyze
        """)
    
    with st.expander("🔀 Job Overlap Analysis - Pre-SAO Optimization"):
        st.markdown("""
        **Identify models being run by multiple jobs** (unnecessary duplication).
        - Job inventory and overlap detection
        - Waste calculation
        - Filter by job types
        
        **Requires**: Environment ID
        **Use case**: Pre-SAO environments to find and eliminate waste
        """)


def show_freshness_analysis():
    """Show detailed freshness configuration analysis."""
    st.header("📋 Model Details")
    st.markdown("Deep dive into individual model configurations and freshness settings from job manifest")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Source selection
    st.subheader("📋 Analysis Source")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        source_mode = st.selectbox(
            "Source",
            ["Environment (Latest)", "Specific Job ID", "Specific Run ID"],
            help="How to select the run to analyze",
            key="freshness_source_mode"
        )
    
    with col2:
        if source_mode == "Specific Job ID":
            job_id_input = st.text_input("Job ID", help="dbt Cloud job ID", key="freshness_job_id")
        elif source_mode == "Specific Run ID":
            run_id_input = st.text_input("Run ID", help="dbt Cloud run ID", key="freshness_run_id")
        else:
            job_id_input = None
            run_id_input = None
    
    with col3:
        job_types_filter = st.multiselect(
            "Filter Job Types",
            options=["ci", "merge", "scheduled", "other"],
            default=["scheduled"],
            help="Filter to specific job types (applies to Environment mode)",
            key="freshness_job_types"
        )
    
    col4, col5 = st.columns(2)
    
    with col4:
        run_status_filter = st.multiselect(
            "Run Status Filter",
            options=["Success", "Error", "Cancelled"],
            default=["Success"],
            help="Filter runs by their completion status",
            key="freshness_run_status"
        )
    
    analyze_button = st.button("🔍 Analyze Freshness", type="primary", key="freshness_analyze")
    
    # Main content area
    if not analyze_button:
        st.info("👈 Configure your settings in the sidebar and click 'Analyze Freshness' to begin")
        
        # Show example output
        st.subheader("📊 Example Output")
        st.markdown("""
        This tool will help you:
        - 📋 View all models and sources with their freshness configuration
        - 📈 See summary statistics on freshness adoption
        - 🔎 Identify which configs come from SQL vs YAML
        - 📊 Track freshness coverage across resource types
        """)
        
        return
    
    # Validation
    if source_mode == "Specific Run ID" and not run_id_input:
        st.error("❌ Please provide a Run ID")
        return
    elif source_mode == "Specific Job ID" and not job_id_input:
        st.error("❌ Please provide a Job ID")
        return
    elif source_mode == "Environment (Latest)" and not config.get('environment_id'):
        st.error("❌ Please configure Environment ID in the Configuration tab")
        return
    
    # Process the analysis
    try:
        with st.spinner("🔄 Processing..."):
            run_id = None
            
            # Handle different source modes
            if source_mode == "Environment (Latest)":
                # Get all jobs in the environment
                jobs_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
                headers = {'Authorization': f'Token {config["api_key"]}'}
                params = {'environment_id': config['environment_id'], 'limit': 100}
                
                jobs_response = requests.get(jobs_url, headers=headers, params=params)
                jobs_response.raise_for_status()
                jobs = jobs_response.json().get('data', [])
                
                # Filter by job type
                jobs = filter_jobs_by_type(jobs, job_types_filter)
                
                if not jobs:
                    st.error(f"❌ No jobs found matching job types: {', '.join(job_types_filter)}")
                    return
                
                # Convert status filter to status codes
                status_codes = []
                if "Success" in run_status_filter:
                    status_codes.append(10)
                if "Error" in run_status_filter:
                    status_codes.append(20)
                if "Cancelled" in run_status_filter:
                    status_codes.append(30)
                
                if not status_codes:
                    st.error("❌ Please select at least one run status to filter by")
                    return
                
                # Get latest run matching status filter from filtered jobs
                latest_run = None
                for job in jobs:
                    runs = get_job_runs(
                        config['api_base'],
                        config['api_key'],
                        config['account_id'],
                        str(job['id']),
                        config.get('environment_id'),
                        limit=1,
                        status=status_codes
                    )
                    
                    if runs:
                        if not latest_run or runs[0]['id'] > latest_run['id']:
                            latest_run = runs[0]
                
                if not latest_run:
                    st.error(f"❌ No runs found matching status [{', '.join(run_status_filter)}] for job types: {', '.join(job_types_filter)}")
                    return
                
                run_id = latest_run['id']
                run_status_humanized = RUN_STATUS_CODES.get(latest_run.get('status'), 'unknown')
                st.info(f"📋 Analyzing latest run from environment: {run_id} (Job: {latest_run.get('job_definition_id')}, Status: {run_status_humanized})")
            
            elif source_mode == "Specific Job ID":
                # Convert status filter to status codes
                status_codes = []
                if "Success" in run_status_filter:
                    status_codes.append(10)
                if "Error" in run_status_filter:
                    status_codes.append(20)
                if "Cancelled" in run_status_filter:
                    status_codes.append(30)
                
                if not status_codes:
                    st.error("❌ Please select at least one run status to filter by")
                    return
                
                # Get latest run from specific job
                runs = get_job_runs(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    job_id_input,
                    config.get('environment_id'),
                    limit=1,
                    status=status_codes
                )
                
                if not runs:
                    st.error(f"❌ No runs found for job {job_id_input} with status: {', '.join(run_status_filter)}")
                    return
                
                run_id = runs[0]['id']
                run_status_humanized = RUN_STATUS_CODES.get(runs[0].get('status'), 'unknown')
                st.info(f"📋 Analyzing latest run from job {job_id_input}: {run_id} (Status: {run_status_humanized})")
            
            elif source_mode == "Specific Run ID":
                run_id = int(run_id_input)
                st.info(f"📋 Analyzing run: {run_id}")
            
            # Now analyze the selected run
            with st.status("Analyzing freshness configuration...") as status:
                status.update(label="Fetching manifest...")
                logger = DBTFreshnessLogger(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    str(run_id)
                )
                
                status.update(label="Processing nodes and sources...")
                results = logger.process_and_log(output_format='json', write_to_db=False)
                
                status.update(label="Analysis complete!", state="complete")
            
            # Display results
            st.success(f"✅ Successfully analyzed {len(results)} items from run {run_id}")
            
            # Calculate summary statistics
            summary = calculate_summary_stats(results)
            
            # Show summary first
            st.header("📊 Summary Statistics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Total Items",
                    summary['overall']['total']
                )
            
            with col2:
                st.metric(
                    "With Freshness",
                    summary['overall']['with_freshness'],
                    delta=f"{summary['overall']['pct_with_freshness']:.1f}%"
                )
            
            with col3:
                st.metric(
                    "Without Freshness",
                    summary['overall']['without_freshness']
                )
            
            with col4:
                st.metric(
                    "Coverage",
                    f"{summary['overall']['pct_with_freshness']:.1f}%"
                )
            
            # Summary tables
            st.subheader("📈 Freshness Coverage by Resource Type")
            st.dataframe(
                summary['by_resource'],
                use_container_width=True,
                hide_index=True
            )
            
            # Package-level breakdown
            if summary.get('by_package_resource') is not None and not summary['by_package_resource'].empty:
                st.subheader("📦 Freshness Coverage by Package & Resource Type")
                st.markdown("*Packages are sorted by size (largest first = main project)*")
                st.dataframe(
                    summary['by_package_resource'],
                    use_container_width=True,
                    hide_index=True
                )
            
            # Detailed results
            st.header("📋 Detailed Results")
            
            # Convert to DataFrame
            df = pd.DataFrame(results)
            
            # Extract package from unique_id if not already present
            if 'package_name' not in df.columns and 'unique_id' in df.columns:
                df['package_name'] = df['unique_id'].apply(lambda x: x.split('.')[1] if isinstance(x, str) and len(x.split('.')) > 1 else 'unknown')
            
            # Add filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Get unique projects/packages
                if 'package_name' in df.columns:
                    unique_projects = sorted(df['package_name'].dropna().unique())
                    project_filter = st.multiselect(
                        "Filter by Project/Package",
                        options=unique_projects,
                        default=unique_projects,
                        help="Select which projects/packages to include"
                    )
                else:
                    project_filter = None
            
            with col2:
                resource_filter = st.multiselect(
                    "Filter by Resource Type",
                    options=sorted(df['resource_type'].unique()),
                    default=df['resource_type'].unique()
                )
            
            with col3:
                has_freshness = st.selectbox(
                    "Has Freshness Config",
                    options=["All", "Yes", "No"]
                )
            
            # Add grouping option
            group_by_project = False
            if 'package_name' in df.columns:
                group_by_project = st.checkbox(
                    "📦 Group by Project/Package",
                    value=False,
                    help="Group models by their project/package"
                )
            
            # Apply filters
            filtered_df = df[df['resource_type'].isin(resource_filter)]
            
            # Apply project filter
            if project_filter is not None and 'package_name' in df.columns:
                filtered_df = filtered_df[filtered_df['package_name'].isin(project_filter)]
            
            if has_freshness == "Yes":
                filtered_df = filtered_df[filtered_df['is_freshness_configured'] == True]
            elif has_freshness == "No":
                filtered_df = filtered_df[filtered_df['is_freshness_configured'] == False]
            
            # Format the dataframe for display
            display_df = filtered_df.copy()
            
            # Reorder columns for better display (include package_name right after resource_type)
            display_columns = ['name', 'resource_type', 'package_name', 'is_freshness_configured',
                'warn_after_count', 'warn_after_period',
                'error_after_count', 'error_after_period',
                'build_after_count', 'build_after_period',
                'updates_on', 'unique_id']
            display_df = display_df[[col for col in display_columns if col in display_df.columns]]
            
            # Rename columns for better display
            column_rename = {
                'name': 'Name',
                'resource_type': 'Resource Type',
                'package_name': 'Project/Package',
                'is_freshness_configured': 'Has Freshness',
                'warn_after_count': 'Warn Count',
                'warn_after_period': 'Warn Period',
                'error_after_count': 'Error Count',
                'error_after_period': 'Error Period',
                'build_after_count': 'Build Count',
                'build_after_period': 'Build Period',
                'updates_on': 'Updates On',
                'unique_id': 'Unique ID'
            }
            display_df = display_df.rename(columns=column_rename)
            
            # Display with or without grouping
            if group_by_project and 'Project/Package' in display_df.columns:
                st.markdown("### Results Grouped by Project/Package")
                
                # Group by project and display each group
                for project in sorted(display_df['Project/Package'].dropna().unique()):
                    project_df = display_df[display_df['Project/Package'] == project].copy()
                    
                    with st.expander(f"📦 **{project}** ({len(project_df)} items)", expanded=True):
                        # Show project-specific stats
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Items", len(project_df))
                        with col2:
                            has_freshness_count = project_df['Has Freshness'].sum() if 'Has Freshness' in project_df.columns else 0
                            st.metric("With Freshness", has_freshness_count)
                        with col3:
                            freshness_pct = (has_freshness_count / len(project_df) * 100) if len(project_df) > 0 else 0
                            st.metric("Coverage", f"{freshness_pct:.1f}%")
                        
                        # Show the dataframe
                        st.dataframe(
                            project_df,
                            use_container_width=True,
                            hide_index=True
                        )
            else:
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True
                )
            
            st.info(f"Showing {len(filtered_df)} of {len(df)} items")
            
            # Download buttons
            st.subheader("💾 Download Results")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Download full results as JSON
                json_str = json.dumps(results, indent=2, default=str)
                st.download_button(
                    label="📥 Download as JSON",
                    data=json_str,
                    file_name=f"freshness_analysis_run_{run_id}.json",
                    mime="application/json"
                )
            
            with col2:
                # Download as CSV
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"freshness_analysis_run_{run_id}.csv",
                    mime="text/csv"
                )
            
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your credentials and IDs")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


def process_single_run(api_base: str, api_key: str, account_id: str, run_id: int, run_created: str, 
                       job_id: str = None, job_name: str = None, run_status: int = None):
    """
    Process a single run to extract model status data.
    
    This function is designed to be called in parallel for multiple runs.
    
    Args:
        api_base: dbt Cloud API base URL
        api_key: dbt Cloud API key
        account_id: dbt Cloud account ID
        run_id: Run ID to process
        run_created: Run created timestamp
        job_id: Job definition ID (optional)
        job_name: Job name (optional)
        run_status: Run invocation status code (optional)
    """
    try:
        logger = DBTFreshnessLogger(api_base, api_key, account_id, str(run_id))
        result = logger.process_and_log(output_format='json', write_to_db=False, include_run_statuses=True)
        
        if result and isinstance(result, dict) and 'run_status_data' in result:
            run_status_data = result['run_status_data']
            
            if run_status_data and 'models' in run_status_data:
                models = []
                for model in run_status_data['models']:
                    model['run_id'] = run_id
                    model['run_created_at'] = run_created
                    model['job_id'] = job_id
                    model['job_name'] = job_name
                    model['run_status'] = run_status
                    models.append(model)
                return {
                    'run_id': run_id, 
                    'success': True, 
                    'models': models,
                    'job_id': job_id,
                    'job_name': job_name,
                    'run_status': run_status
                }
        
        return {'run_id': run_id, 'success': False, 'error': 'No data in response'}
    except Exception as e:
        return {'run_id': run_id, 'success': False, 'error': str(e)}


def show_run_status_analysis():
    """Show historical trends analysis tab."""
    st.header("📈 Historical Trends")
    st.markdown("Analyze performance patterns and execution trends across multiple job runs over time")
    
    st.info("⚡ **Parallel Processing**: Analyzes up to 10 runs simultaneously for 5-10x faster results!")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Analysis parameters
    st.subheader("📋 Analysis Scope")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Default to last 7 days (reduced for performance)
        default_start = datetime.now() - timedelta(days=7)
        default_end = datetime.now()
        
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            help="Start of date range"
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=default_end,
            help="End of date range"
        )
    
    with col3:
        max_runs = st.slider(
            "Max Runs",
            min_value=5,
            max_value=80,
            value=10,
            help="Maximum number of runs to analyze (from total found)"
        )
    
    with col4:
        job_source = st.selectbox(
            "Job Source",
            options=["All Jobs in Environment", "Specific Job ID"],
            help="Analyze all jobs or filter to one"
        )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if job_source == "Specific Job ID":
            job_id_input = st.text_input("Job ID", help="Specific dbt Cloud job ID")
        else:
            job_id_input = None
    
    with col2:
        job_types_filter = st.multiselect(
            "Job Types",
            options=["ci", "merge", "scheduled", "other"],
            default=["scheduled"],
            help="Filter by job type"
        )
    
    col3, col4 = st.columns(2)
    
    with col3:
        run_status_filter = st.multiselect(
            "Run Status",
            options=["Success", "Error", "Cancelled"],
            default=["Success"],
            help="Filter runs by completion status"
        )
    
    with col4:
        sao_only = st.checkbox(
            "SAO Jobs Only",
            value=True,
            help="Only analyze jobs with State-Aware Orchestration enabled (recommended for accurate reuse metrics)",
            key="run_sao_only"
        )
    
    # Performance tip
    st.info("💡 **Performance Tip**: Runs are processed in parallel (up to 10 at a time) for much faster analysis! Feel free to analyze 20-50 runs.")
    
    analyze_button = st.button("📊 Analyze Run Statuses", type="primary", key="run_analyze")
    
    # Main content
    if not analyze_button:
        st.info("⬆️ Set parameters and click 'Analyze Run Statuses' to begin")
        return
    
    # Validation
    if job_source == "Specific Job ID" and not job_id_input:
        st.error("❌ Please provide a Job ID when using 'Specific Job ID' mode")
        return
    elif job_source == "All Jobs in Environment" and not config.get('environment_id'):
        st.error("❌ Please configure Environment ID in the Configuration tab to use 'All Jobs in Environment' mode")
        return
    
    # Convert status filter to status codes
    status_codes = []
    if "Success" in run_status_filter:
        status_codes.append(10)
    if "Error" in run_status_filter:
        status_codes.append(20)
    if "Cancelled" in run_status_filter:
        status_codes.append(30)
    
    if not status_codes:
        st.error("❌ Please select at least one run status to filter by")
        return
    
    try:
        # Convert dates to datetime
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        # Create progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text(f"🔄 Fetching runs from {start_date} to {end_date}...")
        
        # Fetch runs based on job source
        runs = []
        
        if job_source == "All Jobs in Environment":
            # Get all jobs in the environment
            jobs_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
            headers = {'Authorization': f'Token {config["api_key"]}'}
            params = {'environment_id': config['environment_id'], 'limit': 100}
            
            jobs_response = requests.get(jobs_url, headers=headers, params=params)
            jobs_response.raise_for_status()
            jobs = jobs_response.json().get('data', [])
            
            # Filter by job type
            jobs = filter_jobs_by_type(jobs, job_types_filter)
            
            if not jobs:
                st.error(f"❌ No jobs found matching job types: {', '.join(job_types_filter)}")
                progress_bar.empty()
                status_text.empty()
                return
            
            status_text.text(f"🔄 Found {len(jobs)} jobs. Fetching runs...")
            
            # Get runs from all filtered jobs
            # Fetch more than max_runs initially to ensure we have enough after date filtering
            fetch_limit = min(200, max_runs * 3)  # Fetch 3x the slider limit or 200, whichever is smaller
            all_runs = []
            for idx, job in enumerate(jobs):
                job_runs = get_job_runs(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    str(job['id']),
                    config.get('environment_id'),
                    limit=fetch_limit,
                    status=status_codes
                )
                all_runs.extend(job_runs)
                
                # Update progress
                progress = int((idx + 1) / len(jobs) * 10)
                progress_bar.progress(progress)
            
            # Sort by date (don't limit yet - do that after date filtering)
            all_runs = sorted(all_runs, key=lambda x: x.get('created_at', ''), reverse=True)
            runs = all_runs
            
        else:  # Specific Job ID
            # Fetch more runs than the slider to account for date filtering
            fetch_limit = min(200, max_runs * 3)
            runs = get_job_runs(
                config['api_base'],
                config['api_key'],
                config['account_id'],
                job_id_input,
                config.get('environment_id'),
                limit=fetch_limit,
                status=status_codes
            )
        
        # Store count before date filtering
        runs_before_date_filter = len(runs)
        
        # Filter by date
        if start_datetime or end_datetime:
            filtered_runs = []
            for run in runs:
                created_at_str = run.get('created_at')
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        created_at = created_at.replace(tzinfo=None)
                        
                        if start_datetime and created_at < start_datetime:
                            continue
                        if end_datetime and created_at > end_datetime:
                            continue
                        
                        filtered_runs.append(run)
                    except:
                        pass
            runs = filtered_runs
        
        if not runs:
            st.warning(f"No runs found in the specified date range ({start_date} to {end_date})")
            st.info(f"ℹ️ Found {runs_before_date_filter} total runs, but none matched the date range filter.")
            progress_bar.empty()
            status_text.empty()
            return
        
        # Store count after date filtering but before SAO filtering
        runs_after_date_filter = len(runs)
        
        # Filter by SAO if requested
        if sao_only:
            status_text.text(f"🔍 Filtering to SAO-enabled jobs only...")
            sao_runs, non_sao_runs = filter_runs_by_sao(runs)
            runs = sao_runs
            
            if not runs:
                st.warning(f"⚠️ No runs found from SAO-enabled jobs. Found {len(non_sao_runs)} runs from non-SAO jobs.")
                st.info("💡 **Tip**: Uncheck 'SAO Jobs Only' to analyze all jobs, or enable SAO on your jobs for better reuse metrics.")
                progress_bar.empty()
                status_text.empty()
                return
            
            if non_sao_runs:
                st.info(f"ℹ️ Filtered to {len(runs)} SAO-enabled runs (excluded {len(non_sao_runs)} non-SAO runs)")
        
        # Now limit to max_runs AFTER all filtering
        total_runs_found = len(runs)
        runs = runs[:max_runs]
        
        # Better status message
        if total_runs_found > max_runs:
            status_text.text(f"✅ Found {total_runs_found} runs in date range, analyzing {len(runs)} most recent (limited by slider)...")
        else:
            status_text.text(f"✅ Found {total_runs_found} runs in date range. Analyzing all in parallel...")
        progress_bar.progress(10)
        
        # Analyze runs in parallel using ThreadPoolExecutor
        all_model_statuses = []
        completed_runs = 0
        failed_runs = []
        
        # Determine optimal number of workers (max 10 concurrent requests)
        max_workers = min(10, len(runs))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all run processing tasks
            future_to_run = {
                executor.submit(
                    process_single_run,
                config['api_base'],
                config['api_key'],
                config['account_id'],
                    run.get('id'),
                    run.get('created_at'),
                    run.get('job_definition_id'),
                    run.get('job', {}).get('name') if run.get('job') else None,
                    run.get('status')
                ): run.get('id') for run in runs
            }
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_run):
                run_id = future_to_run[future]
                completed_runs += 1
                
                # Update progress
                progress = 10 + int((completed_runs / len(runs)) * 80)
                progress_bar.progress(progress)
                status_text.text(f"🔄 Processed {completed_runs}/{len(runs)} runs (in parallel)...")
                
                try:
                    result = future.result()
                    if result['success']:
                        all_model_statuses.extend(result['models'])
                    else:
                        failed_runs.append((run_id, result.get('error', 'Unknown error')))
                except Exception as e:
                    failed_runs.append((run_id, str(e)))
        
        progress_bar.progress(100)
        status_text.text("✅ Analysis complete!")
        
        # Show warnings for failed runs
        if failed_runs:
            with st.expander(f"⚠️ {len(failed_runs)} run(s) failed to process", expanded=False):
                for run_id, error in failed_runs:
                    st.warning(f"Run {run_id}: {error}")
        
        # Convert to dataframe
        if all_model_statuses:
            df = pd.DataFrame(all_model_statuses)
        else:
            df = pd.DataFrame()
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        
        if df.empty:
            st.warning("No run data found for the specified date range and job")
            return
        
        st.success(f"✅ Analyzed {len(df)} model executions across {df['run_id'].nunique()} runs")
        
        # Diagnostic info
        with st.expander("🔍 Status Breakdown"):
            st.markdown("**Status values found:**")
            status_breakdown = df['status'].value_counts()
            for status, count in status_breakdown.items():
                pct = (count / len(df) * 100) if len(df) > 0 else 0
                st.text(f"  {status}: {count:,} ({pct:.1f}%)")
            
            st.markdown("""
            **How we detect model status:**  
            We use the **step-based approach** to get accurate status from `run_results.json`:
            
            1. **Identify relevant steps**: Filter to only `dbt run` and `dbt build` commands
            2. **Fetch run_results.json**: Get results from each relevant step
            3. **Aggregate results**: Combine data across all steps for accurate counts
            
            This eliminates guesswork and heuristics - we use the actual status from dbt's execution results!
            
            **Status values:**
            - `success`: Model executed successfully
            - `error`: Model execution failed  
            - `skipped`: Model was skipped (often due to deferral/reuse)
            """)
        
        # Summary Statistics
        st.header("📈 Summary Statistics")
        
        # Calculate status counts
        status_counts = df['status'].value_counts()
        total_executions = len(df)
        success_count = status_counts.get('success', 0)
        # Check for both 'skipped' and 'reused' status
        reused_count = status_counts.get('skipped', 0) + status_counts.get('reused', 0)
        error_count = status_counts.get('error', 0)
        
        # Calculate percentages
        success_pct = (success_count / total_executions * 100) if total_executions > 0 else 0
        reused_pct = (reused_count / total_executions * 100) if total_executions > 0 else 0
        error_pct = (error_count / total_executions * 100) if total_executions > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Executions", f"{total_executions:,}")
        
        with col2:
            st.metric("Success", f"{success_count:,}", delta=f"{success_pct:.1f}%")
        
        with col3:
            st.metric("Reused", f"{reused_count:,}", delta=f"{reused_pct:.1f}%")
        
        with col4:
            st.metric("Reuse Rate", f"{reused_pct:.1f}%")
        
        # TRENDING ANALYSIS
        st.divider()
        st.subheader("📈 Reuse Rate Trending")
        
        # Calculate reuse rate per run
        reuse_trending = df.groupby(['run_id', 'run_created_at']).apply(
            lambda x: pd.Series({
                'reuse_rate': (len(x[x['status'].isin(['reused', 'skipped'])]) / len(x) * 100) if len(x) > 0 else 0,
                'total_models': len(x),
                'reused_count': len(x[x['status'].isin(['reused', 'skipped'])]),
                'success_count': len(x[x['status'] == 'success']),
                'error_count': len(x[x['status'] == 'error'])
            })
        ).reset_index()
        
        reuse_trending = reuse_trending.sort_values('run_created_at')
        reuse_trending['run_created_at'] = pd.to_datetime(reuse_trending['run_created_at'])
        
        # Create trending chart
        fig_trending = go.Figure()
        
        # Reuse rate line
        fig_trending.add_trace(go.Scatter(
            x=reuse_trending['run_created_at'],
            y=reuse_trending['reuse_rate'],
            name='Reuse Rate %',
            mode='lines+markers',
            line=dict(color='#17a2b8', width=3),
            marker=dict(size=8)
        ))
        
        # Add 30% goal line
        fig_trending.add_hline(
            y=30,
            line_dash="dash",
            line_color="green",
            annotation_text="30% Goal",
            annotation_position="right"
        )
        
        fig_trending.update_layout(
            title="Reuse Rate Trend Over Time",
            xaxis_title="Date",
            yaxis_title="Reuse Rate (%)",
            hovermode='x unified',
            height=400
        )
        
        st.plotly_chart(fig_trending, use_container_width=True)
        
        # Show statistics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_reuse = reuse_trending['reuse_rate'].mean()
            st.metric("Average Reuse Rate", f"{avg_reuse:.1f}%")
        
        with col2:
            max_reuse = reuse_trending['reuse_rate'].max()
            st.metric("Peak Reuse Rate", f"{max_reuse:.1f}%")
        
        with col3:
            min_reuse = reuse_trending['reuse_rate'].min()
            st.metric("Lowest Reuse Rate", f"{min_reuse:.1f}%")
        
        with col4:
            # Calculate trend (positive/negative)
            if len(reuse_trending) >= 2:
                recent_avg = reuse_trending.tail(5)['reuse_rate'].mean()
                older_avg = reuse_trending.head(5)['reuse_rate'].mean()
                trend = recent_avg - older_avg
                st.metric("Trend", f"{trend:+.1f}%", delta=f"{'Improving' if trend > 0 else 'Declining'}")
            else:
                st.metric("Trend", "N/A")
        
        # Status breakdown by run
        st.divider()
        st.subheader("📊 Status Distribution by Run")
        
        # Group by run and status
        run_status_summary = df.groupby(['run_id', 'run_created_at', 'status']).size().reset_index(name='count')
        
        # Pivot for stacked bar chart
        pivot_df = run_status_summary.pivot_table(
            index=['run_id', 'run_created_at'],
            columns='status',
            values='count',
            fill_value=0
        ).reset_index()
        
        # Sort by date
        pivot_df['run_created_at'] = pd.to_datetime(pivot_df['run_created_at'])
        pivot_df = pivot_df.sort_values('run_created_at')
        
        # Create stacked bar chart
        fig = go.Figure()
        
        # Define colors for each status
        status_colors = {
            'success': '#22c55e',  # green
            'error': '#ef4444',    # red
            'reused': '#60a5fa',   # light blue
            'skipped': '#9ca3af'   # grey
        }
        
        # Add bars for each status
        for status in pivot_df.columns[2:]:  # Skip run_id and run_created_at
            fig.add_trace(go.Bar(
                name=status,
                x=pivot_df['run_created_at'],
                y=pivot_df[status],
                text=pivot_df[status],
                textposition='inside',
                marker_color=status_colors.get(status, '#6b7280')  # default to gray if status not found
            ))
        
        fig.update_layout(
            title="Model Execution Status by Run",
            xaxis_title="Run Date",
            yaxis_title="Number of Models",
            barmode='stack',
            hovermode='x unified',
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed breakdown table
        st.subheader("📋 Detailed Run Breakdown")
        
        st.markdown("""
        **Status values** are extracted from `run_results.json` using a step-based approach that filters to only `dbt run` and `dbt build` commands.
        """)
        
        # Create summary by run
        run_summary = df.groupby(['run_id', 'run_created_at']).agg({
            'status': lambda x: x.value_counts().to_dict(),
            'job_id': 'first',
            'job_name': 'first',
            'run_status': 'first'
        }).reset_index()
        
        # Calculate execution time stats separately
        exec_time_stats = df.groupby(['run_id']).agg({
            'execution_time': ['mean', 'median']
        }).reset_index()
        exec_time_stats.columns = ['run_id', 'avg_exec_time', 'median_exec_time']
        
        # Merge execution time stats
        run_summary = run_summary.merge(exec_time_stats, on='run_id', how='left')
        
        # Map run_status codes to human-readable names
        run_summary['run_status_name'] = run_summary['run_status'].map(RUN_STATUS_CODES)
        
        # Expand status counts
        status_columns = []
        for status in df['status'].unique():
            run_summary[status] = run_summary['status'].apply(lambda x: x.get(status, 0))
            status_columns.append(status)
        
        # Calculate totals and percentages
        run_summary['total'] = run_summary[status_columns].sum(axis=1)
        
        # Calculate reuse rate if we have skipped or reused statuses
        has_reuse_column = False
        if 'skipped' in status_columns or 'reused' in status_columns:
            # Sum skipped and reused columns (handle if they don't exist)
            skipped_col = run_summary['skipped'] if 'skipped' in status_columns else 0
            reused_col_data = run_summary['reused'] if 'reused' in status_columns else 0
            total_reused = skipped_col + reused_col_data
            run_summary['reuse_rate_%'] = (total_reused / run_summary['total'] * 100).round(1)
            has_reuse_column = True
        
        # Format for display
        display_columns = ['run_created_at', 'run_id', 'job_id', 'job_name', 'run_status_name', 'total'] + status_columns
        if has_reuse_column:
            display_columns.append('reuse_rate_%')
        
        # Add execution time to display columns
        display_columns.extend(['avg_exec_time', 'median_exec_time'])
        
        display_summary = run_summary[display_columns].copy()
        display_summary['run_created_at'] = pd.to_datetime(display_summary['run_created_at']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Round execution times
        display_summary['avg_exec_time'] = display_summary['avg_exec_time'].round(3)
        display_summary['median_exec_time'] = display_summary['median_exec_time'].round(3)
        
        # Set column names - must match display_columns order
        new_column_names = ['Run Date', 'Run ID', 'Job ID', 'Job Name', 'Run Status', 'Total'] + [s.title() for s in status_columns]
        if has_reuse_column:
            new_column_names.append('Reuse Rate %')
        new_column_names.extend(['Avg Time (s)', 'Median Time (s)'])
        display_summary.columns = new_column_names
        
        st.dataframe(display_summary, width='stretch', hide_index=True)
        
        # Download option
        st.subheader("💾 Download Data")
        csv = df.to_csv(index=False)
        
        # Generate filename based on job source
        if job_source == "All Jobs in Environment":
            filename_suffix = f"environment_{config.get('environment_id', 'all')}"
        else:
            filename_suffix = f"job_{job_id_input}"
        
        st.download_button(
            label="📥 Download Full Run Data as CSV",
            data=csv,
            file_name=f"run_status_analysis_{filename_suffix}_{start_date}_to_{end_date}.csv",
            mime="text/csv"
        )
        
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your credentials and IDs")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


def fetch_all_models_graphql(api_key: str, environment_id: int, page_size: int = 500):
    """
    Fetch all models from dbt Cloud using GraphQL API.
    Returns a list of model nodes with execution info and config.
    """
    url = 'https://metadata.cloud.getdbt.com/graphql'
    headers = {'Authorization': f'Bearer {api_key}'}
    
    query_body = '''
    query Environment($environmentId: BigInt!, $first: Int, $after: String) {
      environment(id: $environmentId) {
        applied {
          models(first: $first, after: $after) {
            pageInfo {
              startCursor
              endCursor
              hasNextPage
            }
            edges {
              node {
                config
                name
                packageName
                resourceType
                executionInfo {
                  lastRunGeneratedAt
                  lastRunStatus
                  lastSuccessJobDefinitionId
                  lastSuccessRunId
                  lastRunError
                  executeCompletedAt
                }
              }
            }
          }
        }
      }
    }
    '''
    
    all_nodes = []
    has_next_page = True
    cursor = None
    page_count = 0
    
    while has_next_page:
        page_count += 1
        
        variables = {
            "environmentId": environment_id,
            "first": page_size
        }
        
        if cursor:
            variables["after"] = cursor
        
        try:
            response = requests.post(
                url, 
                headers=headers, 
                json={"query": query_body, "variables": variables}
            )
            response.raise_for_status()
            response_data = response.json()
            
            if "errors" in response_data:
                st.error(f"GraphQL errors: {response_data['errors']}")
                break
            
            models_data = response_data.get("data", {}).get("environment", {}).get("applied", {}).get("models", {})
            
            if not models_data:
                break
            
            page_info = models_data.get("pageInfo", {})
            edges = models_data.get("edges", [])
            
            if not edges:
                break
            
            nodes = [edge['node'] for edge in edges]
            all_nodes.extend(nodes)
            
            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
            
            if has_next_page and not cursor:
                break
                
        except requests.exceptions.RequestException as e:
            st.error(f"Request error: {e}")
            break
        except KeyError as e:
            st.error(f"Error parsing response: {e}")
            break
    
    return all_nodes


def show_model_reuse_slo_analysis():
    """Show Environment Overview tab using GraphQL API."""
    st.header("🎯 Environment Overview")
    st.markdown("**Your main dashboard** for environment health, performance, and SLO compliance using real-time dbt Cloud data")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Environment ID input
    col1, col2 = st.columns([2, 1])
    
    with col1:
        environment_id_input = st.text_input(
            "Environment ID",
            value=config.get('environment_id', ''),
            help="dbt Cloud environment ID for GraphQL queries",
            key="reuse_environment_id"
        )
    
    with col2:
        st.markdown("")  # Spacing
        st.markdown("")  # Spacing
        analyze_button = st.button("🔍 Analyze Environment", type="primary", key="reuse_analyze")
    
    if not analyze_button:
        st.info("⬆️ Enter an Environment ID and click 'Analyze Environment' to begin")
        st.markdown("""
        ### What This Analysis Provides:
        - 📊 **SLO Compliance Table**: View models with their freshness config and current execution status
        - 📈 **Build After Distribution**: See how models are distributed by build_after configuration
        - 🔄 **Status Distribution**: Breakdown of reused vs success vs error statuses
        - 🎯 **Reuse Percentage**: Current total reuse rate for the environment
        """)
        return
    
    if not environment_id_input:
        st.error("❌ Please provide an Environment ID")
        return
    
    try:
        environment_id = int(environment_id_input)
    except ValueError:
        st.error("❌ Environment ID must be a number")
        return
    
    # Fetch data
    try:
        with st.spinner("🔄 Fetching models from GraphQL API..."):
            all_nodes = fetch_all_models_graphql(config['api_key'], environment_id)
        
        if not all_nodes:
            st.warning("No models found for this environment")
            return
        
        st.success(f"✅ Fetched {len(all_nodes)} models from environment {environment_id}")
        
        # Convert to DataFrame
        df = pd.DataFrame(all_nodes)
        
        # Parse config JSON
        import ast
        df['config'] = df['config'].apply(lambda x: json.loads(ast.literal_eval(x)) if isinstance(x, str) else x)
        
        # Extract nested executionInfo fields
        df['last_run_generated_at'] = df['executionInfo'].apply(lambda x: x.get('lastRunGeneratedAt') if x else None)
        df['execute_completed_at'] = df['executionInfo'].apply(lambda x: x.get('executeCompletedAt') if x else None)
        df['last_run_status'] = df['executionInfo'].apply(lambda x: x.get('lastRunStatus') if x else None)
        df['last_job_id'] = df['executionInfo'].apply(lambda x: x.get('lastSuccessJobDefinitionId') if x else None)
        df['last_run_id'] = df['executionInfo'].apply(lambda x: x.get('lastSuccessRunId') if x else None)
        df['last_run_error'] = df['executionInfo'].apply(lambda x: x.get('lastRunError') if x else None)
        
        # Extract freshness config from config JSON
        df['build_after_count'] = df['config'].apply(lambda x: x.get('freshness', {}).get('build_after', {}).get('count') if isinstance(x, dict) else None)
        df['build_after_period'] = df['config'].apply(lambda x: x.get('freshness', {}).get('build_after', {}).get('period') if isinstance(x, dict) else None)
        df['updates_on'] = df['config'].apply(lambda x: x.get('freshness', {}).get('build_after', {}).get('updates_on') if isinstance(x, dict) else None)
        df['materialization'] = df['config'].apply(lambda x: x.get('materialized') if isinstance(x, dict) else None)
        
        # Convert timestamps and remove timezone info for calculations
        df['last_run_generated_at'] = pd.to_datetime(df['last_run_generated_at'], errors='coerce', utc=True).dt.tz_localize(None)
        df['execute_completed_at'] = pd.to_datetime(df['execute_completed_at'], errors='coerce', utc=True).dt.tz_localize(None)
        
        # Calculate hours since last execution
        df['hours_since_last_execution'] = (datetime.now() - df['execute_completed_at']).dt.total_seconds() / 3600
        
        # Calculate expected hours between runs
        df['expected_hours_between_runs'] = df.apply(
            lambda row: (
                row['build_after_count'] * 24 if row['build_after_period'] == 'day'
                else row['build_after_count'] if row['build_after_period'] == 'hour'
                else None
            ),
            axis=1
        )
        
        # Determine if outside of SLO
        df['is_outside_of_slo'] = (
            (df['hours_since_last_execution'] > df['expected_hours_between_runs']) &
            (df['expected_hours_between_runs'].notna())
        )
        
        # Display key metrics
        st.divider()
        st.subheader("🎯 Key Metrics")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_models = len(df)
            st.metric("Total Models", f"{total_models:,}")
        
        with col2:
            # Calculate reuse rate (models with 'reused' status)
            reused_count = len(df[df['last_run_status'] == 'reused'])
            reuse_pct = (reused_count / total_models * 100) if total_models > 0 else 0
            st.metric("Reuse Rate", f"{reuse_pct:.1f}%", delta=f"{reused_count:,} models")
        
        with col3:
            # Models with freshness config
            has_freshness = df['build_after_count'].notna().sum()
            freshness_pct = (has_freshness / total_models * 100) if total_models > 0 else 0
            st.metric("Has Freshness Config", f"{freshness_pct:.1f}%", delta=f"{has_freshness:,} models")
        
        with col4:
            # Models outside SLO
            outside_slo = df['is_outside_of_slo'].sum()
            slo_pct = (outside_slo / total_models * 100) if total_models > 0 else 0
            st.metric("Outside SLO", f"{slo_pct:.1f}%", delta=f"{outside_slo:,} models")
        
        # State-Aware Orchestration analysis
        st.divider()
        st.subheader("🔄 State-Aware Orchestration (SAO) Adoption")
        
        with st.spinner("🔄 Analyzing jobs for SAO features..."):
            # Fetch all jobs for this environment
            jobs_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
            headers = {'Authorization': f'Token {config["api_key"]}'}
            params = {'limit': 100, 'environment_id': environment_id}
            
            try:
                jobs_response = requests.get(jobs_url, headers=headers, params=params)
                jobs_response.raise_for_status()
                jobs = jobs_response.json().get('data', [])
                
                if jobs:
                    # Count SAO-enabled jobs
                    sao_jobs = [job for job in jobs if check_job_has_sao(job)]
                    total_jobs = len(jobs)
                    sao_count = len(sao_jobs)
                    sao_pct = (sao_count / total_jobs * 100) if total_jobs > 0 else 0
                    
                    # Display metrics
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.metric("Total Jobs", f"{total_jobs:,}")
                    
                    with col2:
                        st.metric("SAO-Enabled Jobs", f"{sao_count:,}", delta=f"{sao_pct:.1f}%")
                    
                    with col3:
                        st.metric("Non-SAO Jobs", f"{total_jobs - sao_count:,}", delta=f"{100 - sao_pct:.1f}%")
                    
                    # Create visualization
                    st.markdown("#### SAO Adoption Distribution")
                    
                    # Prepare data for chart
                    chart_data = pd.DataFrame({
                        'Category': ['SAO-Enabled', 'Non-SAO'],
                        'Count': [sao_count, total_jobs - sao_count],
                        'Percentage': [sao_pct, 100 - sao_pct]
                    })
                    
                    # Create two columns for charts
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Pie chart
                        fig_pie = px.pie(
                            chart_data, 
                            values='Count', 
                            names='Category',
                            title='SAO Adoption by Job Count',
                            color='Category',
                            color_discrete_map={'SAO-Enabled': '#10b981', 'Non-SAO': '#6b7280'},
                            hole=0.4
                        )
                        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                        st.plotly_chart(fig_pie, use_container_width=True)
                    
                    with col2:
                        # Bar chart
                        fig_bar = px.bar(
                            chart_data,
                            x='Category',
                            y='Count',
                            title='SAO Adoption by Job Count',
                            color='Category',
                            color_discrete_map={'SAO-Enabled': '#10b981', 'Non-SAO': '#6b7280'},
                            text='Count'
                        )
                        fig_bar.update_traces(textposition='outside')
                        fig_bar.update_layout(showlegend=False, yaxis_title='Number of Jobs')
                        st.plotly_chart(fig_bar, use_container_width=True)
                    
                    # Scheduled Jobs SAO Analysis
                    st.markdown("---")
                    st.markdown("#### SAO Adoption for Scheduled Jobs")
                    
                    scheduled_jobs = [job for job in jobs if determine_job_type(job.get('triggers', {})) == 'scheduled']
                    
                    if scheduled_jobs:
                        scheduled_sao_jobs = [job for job in scheduled_jobs if check_job_has_sao(job)]
                        total_scheduled = len(scheduled_jobs)
                        scheduled_sao_count = len(scheduled_sao_jobs)
                        scheduled_sao_pct = (scheduled_sao_count / total_scheduled * 100) if total_scheduled > 0 else 0
                        
                        col1, col2 = st.columns([1, 2])
                        
                        with col1:
                            # Metrics for scheduled jobs
                            st.metric("Total Scheduled Jobs", f"{total_scheduled:,}")
                            st.metric("SAO-Enabled", f"{scheduled_sao_count:,}", delta=f"{scheduled_sao_pct:.1f}%")
                            st.metric("Non-SAO", f"{total_scheduled - scheduled_sao_count:,}", delta=f"{100 - scheduled_sao_pct:.1f}%")
                        
                        with col2:
                            # Pie chart for scheduled jobs
                            scheduled_chart_data = pd.DataFrame({
                                'Category': ['SAO-Enabled', 'Non-SAO'],
                                'Count': [scheduled_sao_count, total_scheduled - scheduled_sao_count],
                                'Percentage': [scheduled_sao_pct, 100 - scheduled_sao_pct]
                            })
                            
                            fig_scheduled_pie = px.pie(
                                scheduled_chart_data,
                                values='Count',
                                names='Category',
                                title=f'SAO Adoption for Scheduled Jobs ({total_scheduled} total)',
                                color='Category',
                                color_discrete_map={'SAO-Enabled': '#10b981', 'Non-SAO': '#6b7280'},
                                hole=0.4
                            )
                            fig_scheduled_pie.update_traces(textposition='inside', textinfo='percent+label+value')
                            st.plotly_chart(fig_scheduled_pie, use_container_width=True)
                        
                        # Show recommendation if scheduled SAO adoption is low
                        if scheduled_sao_pct < 50:
                            st.warning(f"""
                            ⚠️ **Low SAO Adoption in Scheduled Jobs**: Only **{scheduled_sao_pct:.1f}%** ({scheduled_sao_count}/{total_scheduled}) of your scheduled production jobs use SAO.
                            
                            Scheduled jobs typically benefit most from SAO since they run repeatedly on the same data.
                            """)
                    else:
                        st.info("No scheduled jobs found in this environment")
                    
                    # ========== NEW FEATURE 1: Job Type Breakdown ==========
                    st.markdown("---")
                    st.markdown("#### SAO Adoption by Job Type")
                    st.markdown("Compare SAO adoption across CI, Merge, Scheduled, and Other job types")
                    
                    # Categorize all jobs by type and SAO status
                    job_type_breakdown = {
                        'ci': {'sao': 0, 'non_sao': 0},
                        'merge': {'sao': 0, 'non_sao': 0},
                        'scheduled': {'sao': 0, 'non_sao': 0},
                        'other': {'sao': 0, 'non_sao': 0}
                    }
                    
                    for job in jobs:
                        job_type = determine_job_type(job.get('triggers', {}))
                        has_sao = check_job_has_sao(job)
                        
                        if has_sao:
                            job_type_breakdown[job_type]['sao'] += 1
                        else:
                            job_type_breakdown[job_type]['non_sao'] += 1
                    
                    # Create DataFrame for visualization
                    breakdown_data = []
                    for job_type, counts in job_type_breakdown.items():
                        total = counts['sao'] + counts['non_sao']
                        if total > 0:  # Only include types that exist
                            breakdown_data.append({
                                'Job Type': job_type.upper(),
                                'SAO-Enabled': counts['sao'],
                                'Non-SAO': counts['non_sao'],
                                'Total': total,
                                'SAO %': (counts['sao'] / total * 100) if total > 0 else 0
                            })
                    
                    if breakdown_data:
                        breakdown_df = pd.DataFrame(breakdown_data)
                        
                        # Create grouped bar chart
                        fig_breakdown = go.Figure()
                        
                        fig_breakdown.add_trace(go.Bar(
                            name='SAO-Enabled',
                            x=breakdown_df['Job Type'],
                            y=breakdown_df['SAO-Enabled'],
                            marker_color='#10b981',
                            text=breakdown_df['SAO-Enabled'],
                            textposition='auto',
                        ))
                        
                        fig_breakdown.add_trace(go.Bar(
                            name='Non-SAO',
                            x=breakdown_df['Job Type'],
                            y=breakdown_df['Non-SAO'],
                            marker_color='#6b7280',
                            text=breakdown_df['Non-SAO'],
                            textposition='auto',
                        ))
                        
                        fig_breakdown.update_layout(
                            title='SAO Adoption by Job Type',
                            xaxis_title='Job Type',
                            yaxis_title='Number of Jobs',
                            barmode='group',
                            height=400,
                            showlegend=True,
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            )
                        )
                        
                        st.plotly_chart(fig_breakdown, use_container_width=True)
                        
                        # Show summary table with percentages
                        col1, col2 = st.columns([2, 1])
                        
                        with col1:
                            st.dataframe(
                                breakdown_df.style.format({
                                    'SAO %': '{:.1f}%'
                                }),
                                use_container_width=True,
                                hide_index=True
                            )
                        
                        with col2:
                            # Highlight key insights
                            ci_jobs = breakdown_df[breakdown_df['Job Type'] == 'CI']
                            if not ci_jobs.empty and ci_jobs.iloc[0]['SAO %'] < 50:
                                st.warning("⚠️ **CI Jobs**: Low SAO adoption. CI jobs benefit greatly from SAO!")
                            
                            merge_jobs = breakdown_df[breakdown_df['Job Type'] == 'MERGE']
                            if not merge_jobs.empty and merge_jobs.iloc[0]['SAO %'] < 50:
                                st.warning("⚠️ **Merge Jobs**: Consider enabling SAO for faster deploys")
                    
                    # ========== NEW FEATURE 2: Freshness Config Coverage ==========
                    st.markdown("---")
                    st.markdown("#### Freshness Configuration Coverage")
                    st.markdown("SAO works best when models have freshness configs. Identify misconfigurations:")
                    
                    # Analyze freshness config for each job
                    jobs_sao_no_freshness = []
                    jobs_freshness_no_sao = []
                    jobs_both = []
                    jobs_neither = []
                    
                    with st.spinner("🔄 Analyzing job configurations..."):
                        for job in jobs:
                            job_id = job['id']
                            job_name = job['name']
                            job_type = determine_job_type(job.get('triggers', {}))
                            has_sao = check_job_has_sao(job)
                            
                            # Check if job has any freshness-related settings
                            # We'll check the execute_steps for freshness-related commands
                            execute_steps = job.get('execute_steps', [])
                            has_freshness = any('freshness' in str(step).lower() for step in execute_steps)
                            
                            # Also check settings for common freshness indicators
                            settings = job.get('settings', {})
                            if not has_freshness:
                                # Check for dbt build command which respects freshness
                                has_freshness = any('dbt build' in str(step) for step in execute_steps)
                            
                            job_info = {
                                'Job ID': job_id,
                                'Job Name': job_name,
                                'Job Type': job_type.upper(),
                            }
                            
                            if has_sao and has_freshness:
                                jobs_both.append(job_info)
                            elif has_sao and not has_freshness:
                                jobs_sao_no_freshness.append(job_info)
                            elif not has_sao and has_freshness:
                                jobs_freshness_no_sao.append(job_info)
                            else:
                                jobs_neither.append(job_info)
                    
                    # Create summary metrics
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("✅ Both SAO & Freshness", len(jobs_both), help="Optimal configuration")
                    
                    with col2:
                        st.metric("⚠️ SAO without Freshness", len(jobs_sao_no_freshness), help="May not reuse effectively")
                    
                    with col3:
                        st.metric("💡 Freshness without SAO", len(jobs_freshness_no_sao), help="Missing optimization")
                    
                    with col4:
                        st.metric("❌ Neither", len(jobs_neither), help="Basic configuration")
                    
                    # Visual breakdown
                    config_data = pd.DataFrame({
                        'Configuration': [
                            '✅ Both',
                            '⚠️ SAO Only',
                            '💡 Freshness Only',
                            '❌ Neither'
                        ],
                        'Count': [
                            len(jobs_both),
                            len(jobs_sao_no_freshness),
                            len(jobs_freshness_no_sao),
                            len(jobs_neither)
                        ],
                        'Status': ['Optimal', 'Suboptimal', 'Opportunity', 'Basic']
                    })
                    
                    fig_config = px.bar(
                        config_data,
                        x='Configuration',
                        y='Count',
                        title='Job Configuration Coverage',
                        color='Status',
                        color_discrete_map={
                            'Optimal': '#10b981',
                            'Suboptimal': '#f59e0b',
                            'Opportunity': '#3b82f6',
                            'Basic': '#6b7280'
                        },
                        text='Count'
                    )
                    fig_config.update_traces(textposition='outside')
                    fig_config.update_layout(showlegend=True, yaxis_title='Number of Jobs', height=400)
                    st.plotly_chart(fig_config, use_container_width=True)
                    
                    # Show problematic configurations in expanders
                    if jobs_sao_no_freshness:
                        with st.expander(f"⚠️ {len(jobs_sao_no_freshness)} Jobs with SAO but NO Freshness Config (May Not Reuse!)"):
                            st.warning("These jobs have SAO enabled but may not use freshness checks. Without freshness configs, models may not reuse effectively.")
                            st.dataframe(pd.DataFrame(jobs_sao_no_freshness), use_container_width=True, hide_index=True)
                    
                    if jobs_freshness_no_sao:
                        with st.expander(f"💡 {len(jobs_freshness_no_sao)} Jobs with Freshness but NO SAO (Optimization Opportunity!)"):
                            st.info("These jobs check freshness but don't have SAO enabled. Enable SAO to skip unchanged models and reduce costs.")
                            st.dataframe(pd.DataFrame(jobs_freshness_no_sao), use_container_width=True, hide_index=True)
                    
                    # ========== NEW FEATURE 3: Top Opportunities ==========
                    st.markdown("---")
                    st.markdown("#### 🎯 Top Opportunities: Jobs That Should Enable SAO")
                    st.markdown("Prioritize enabling SAO on these jobs for maximum impact:")
                    
                    # Fetch run data for non-SAO jobs to calculate potential impact
                    non_sao_jobs = [job for job in jobs if not check_job_has_sao(job)]
                    
                    if non_sao_jobs:
                        opportunities = []
                        
                        with st.spinner(f"🔄 Analyzing {len(non_sao_jobs)} non-SAO jobs for optimization potential..."):
                            for job in non_sao_jobs[:20]:  # Limit to top 20 for performance
                                job_id = job['id']
                                job_name = job['name']
                                job_type = determine_job_type(job.get('triggers', {}))
                                
                                try:
                                    # Fetch recent runs for this job
                                    runs_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/'
                                    run_params = {
                                        'job_definition_id': job_id,
                                        'limit': 10,
                                        'order_by': '-id',
                                        'status': '10'  # Success only
                                    }
                                    
                                    runs_response = requests.get(runs_url, headers=headers, params=run_params)
                                    runs_response.raise_for_status()
                                    runs = runs_response.json().get('data', [])
                                    
                                    if runs:
                                        # Calculate metrics
                                        run_count = len(runs)
                                        avg_duration = sum(r.get('duration_humanized_seconds', 0) or 0 for r in runs) / run_count if run_count > 0 else 0
                                        avg_duration_mins = avg_duration / 60
                                        
                                        # Calculate potential impact score
                                        # Higher score = more frequent + longer running = bigger opportunity
                                        impact_score = run_count * avg_duration_mins
                                        
                                        # Priority based on job type
                                        priority = 'High' if job_type in ['ci', 'merge'] else 'Medium' if job_type == 'scheduled' else 'Low'
                                        
                                        opportunities.append({
                                            'Job ID': job_id,
                                            'Job Name': job_name,
                                            'Job Type': job_type.upper(),
                                            'Recent Runs': run_count,
                                            'Avg Duration (min)': round(avg_duration_mins, 2),
                                            'Impact Score': round(impact_score, 2),
                                            'Priority': priority
                                        })
                                
                                except Exception as e:
                                    # Skip jobs that error
                                    continue
                        
                        if opportunities:
                            opp_df = pd.DataFrame(opportunities)
                            # Sort by impact score (descending)
                            opp_df = opp_df.sort_values('Impact Score', ascending=False)
                            
                            # Show top 10
                            st.markdown("**Top 10 Jobs by Potential Impact:**")
                            
                            # Style the dataframe with colored text instead of highlighting
                            def color_priority(row):
                                if row['Priority'] == 'High':
                                    return ['color: #dc2626; font-weight: bold'] * len(row)  # Red text
                                elif row['Priority'] == 'Medium':
                                    return ['color: #d97706; font-weight: bold'] * len(row)  # Orange text
                                else:
                                    return ['color: #6b7280'] * len(row)  # Gray text
                            
                            styled_df = opp_df.head(10).style.apply(color_priority, axis=1)
                            st.dataframe(styled_df, use_container_width=True, hide_index=True)
                            
                            # Show calculation explanation
                            st.info("""
                            **Impact Score Calculation**: `Recent Runs × Avg Duration (minutes)`
                            
                            Higher scores indicate jobs that run frequently and take longer, making them prime candidates for SAO optimization.
                            
                            **Priority Levels:**
                            - 🔴 **High**: CI and Merge jobs (run most frequently)
                            - 🟡 **Medium**: Scheduled jobs (regular cadence)
                            - ⚪ **Low**: Other job types
                            """)
                            
                            # Estimated savings
                            total_impact = opp_df['Impact Score'].sum()
                            st.success(f"""
                            **💰 Estimated Opportunity**: If SAO reduces run time by 30-50% on these jobs:
                            - Potential time savings: **{total_impact * 0.4:.0f} minutes** across recent runs
                            - Focus on high-impact jobs first for maximum ROI
                            """)
                        else:
                            st.info("No run data available for non-SAO jobs to calculate opportunities")
                    else:
                        st.success("🎉 All jobs have SAO enabled! No optimization opportunities.")
                    
                    # Show list of all jobs with SAO status in expander
                    if jobs:
                        with st.expander(f"📋 View All {len(jobs)} Jobs (SAO Status)"):
                            all_jobs_df = pd.DataFrame([
                                {
                                    'Job ID': job['id'],
                                    'Job Name': job['name'],
                                    'Job Type': determine_job_type(job.get('triggers', {})),
                                    'SAO Enabled': '✅ Yes' if check_job_has_sao(job) else '❌ No',
                                    'Environment ID': job.get('environment_id')
                                }
                                for job in jobs
                            ])
                            # Sort by SAO status (enabled first) then by job name
                            all_jobs_df = all_jobs_df.sort_values(['SAO Enabled', 'Job Name'], ascending=[False, True])
                            st.dataframe(all_jobs_df, use_container_width=True, hide_index=True)
                    
                    # Show recommendations if SAO adoption is low
                    if sao_pct < 50:
                        st.info(f"""
                        💡 **Recommendation**: Consider enabling State-Aware Orchestration (SAO) on more jobs to improve efficiency.
                        
                        Current adoption: **{sao_pct:.1f}%** ({sao_count}/{total_jobs} jobs)
                        
                        SAO benefits:
                        - Skips unchanged models, reducing compute costs
                        - Faster run times by only building what's necessary
                        - Better resource utilization across your environment
                        """)
                else:
                    st.warning("No jobs found for this environment")
                    
            except requests.exceptions.RequestException as e:
                st.error(f"❌ Error fetching jobs: {str(e)}")
        
        # TODO 1: SLO Compliance Table
        st.divider()
        st.subheader("📊 SLO Compliance Table")
        
        # Create display table
        display_df = df[[
            'name', 'packageName', 'resourceType', 'last_run_generated_at',
            'execute_completed_at', 'hours_since_last_execution', 'last_run_status',
            'last_job_id', 'last_run_id', 'build_after_count', 'build_after_period',
            'updates_on', 'materialization', 'expected_hours_between_runs', 'is_outside_of_slo'
        ]].copy()
        
        # Round numeric columns
        display_df['hours_since_last_execution'] = display_df['hours_since_last_execution'].round(2)
        display_df['expected_hours_between_runs'] = display_df['expected_hours_between_runs'].round(2)
        
        # Rename columns for display
        display_df.columns = [
            'Name', 'Package', 'Type', 'Last Run Generated',
            'Last Execution', 'Hours Since Exec', 'Status',
            'Job ID', 'Run ID', 'Build Count', 'Build Period',
            'Updates On', 'Materialization', 'Expected Hours', 'Outside SLO'
        ]
        
        # Filter options
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.multiselect(
                "Filter by Status",
                options=sorted(df['last_run_status'].dropna().unique()),
                default=None,
                key="reuse_status_filter"
            )
        
        with col2:
            slo_filter = st.selectbox(
                "SLO Status",
                options=["All", "Within SLO", "Outside SLO"],
                key="reuse_slo_filter"
            )
        
        with col3:
            has_freshness_filter = st.selectbox(
                "Freshness Config",
                options=["All", "Has Config", "No Config"],
                key="reuse_freshness_filter"
            )
        
        # Apply filters
        filtered_display = display_df.copy()
        
        if status_filter:
            filtered_display = filtered_display[filtered_display['Status'].isin(status_filter)]
        
        if slo_filter == "Within SLO":
            filtered_display = filtered_display[filtered_display['Outside SLO'] == False]
        elif slo_filter == "Outside SLO":
            filtered_display = filtered_display[filtered_display['Outside SLO'] == True]
        
        if has_freshness_filter == "Has Config":
            filtered_display = filtered_display[filtered_display['Build Count'].notna()]
        elif has_freshness_filter == "No Config":
            filtered_display = filtered_display[filtered_display['Build Count'].isna()]
        
        # Style the dataframe
        st.dataframe(
            filtered_display,
            width='stretch',
            hide_index=True,
            column_config={
                "Outside SLO": st.column_config.CheckboxColumn("Outside SLO")
            }
        )
        
        st.caption(f"Showing {len(filtered_display):,} of {len(display_df):,} models")
        
        # TODO 2: Model Distribution by Build After Configuration
        st.divider()
        st.subheader("📈 Model Distribution by Build After Configuration")
        
        # Filter models with freshness config
        models_with_config = df[df['build_after_count'].notna()].copy()
        
        if len(models_with_config) > 0:
            # Create a combined label for build_after
            models_with_config['build_after_label'] = models_with_config.apply(
                lambda row: f"{int(row['build_after_count'])} {row['build_after_period']}(s)" 
                if pd.notna(row['build_after_count']) and pd.notna(row['build_after_period'])
                else 'Unknown',
                axis=1
            )
            
            # Count by build_after configuration
            config_counts = models_with_config['build_after_label'].value_counts().reset_index()
            config_counts.columns = ['Build After Config', 'Count']
            
            # Create bar chart
            fig = px.bar(
                config_counts,
                x='Build After Config',
                y='Count',
                title='Model Count by Build After Configuration',
                text='Count',
                color='Count',
                color_continuous_scale='Blues'
            )
            
            fig.update_traces(textposition='outside')
            fig.update_layout(
                xaxis_title="Build After Configuration",
                yaxis_title="Number of Models",
                showlegend=False,
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Show breakdown table
            col1, col2 = st.columns(2)
            
            with col1:
                st.dataframe(config_counts, width='stretch', hide_index=True)
            
            with col2:
                # Pie chart
                fig_pie = px.pie(
                    config_counts,
                    names='Build After Config',
                    values='Count',
                    title='Build After Configuration Distribution'
                )
                st.plotly_chart(fig_pie, width='stretch')
        else:
            st.info("No models have freshness build_after configuration")
        
        # TODO 3: Distribution of Statuses
        st.divider()
        st.subheader("🔄 Distribution of Statuses (Reused vs Success vs Error)")
        
        # Count by status
        status_counts = df['last_run_status'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Bar chart
            fig_status_bar = px.bar(
                status_counts,
                x='Status',
                y='Count',
                title='Model Count by Execution Status',
                text='Count',
                color='Status',
                color_discrete_map={
                    'success': '#28a745',
                    'reused': '#17a2b8',
                    'error': '#dc3545',
                    'skipped': '#ffc107'
                }
            )
            
            fig_status_bar.update_traces(textposition='outside')
            fig_status_bar.update_layout(
                xaxis_title="Status",
                yaxis_title="Number of Models",
                showlegend=False,
                height=400
            )
            
            st.plotly_chart(fig_status_bar, width='stretch')
        
        with col2:
            # Pie chart
            fig_status_pie = px.pie(
                status_counts,
                names='Status',
                values='Count',
                title='Status Distribution',
                color='Status',
                color_discrete_map={
                    'success': '#28a745',
                    'reused': '#17a2b8',
                    'error': '#dc3545',
                    'skipped': '#ffc107'
                }
            )
            st.plotly_chart(fig_status_pie, width='stretch')
        
        # Status breakdown table
        status_counts['Percentage'] = (status_counts['Count'] / status_counts['Count'].sum() * 100).round(1)
        status_counts['Percentage'] = status_counts['Percentage'].astype(str) + '%'
        
        st.dataframe(status_counts, width='stretch', hide_index=True)
        
        # TODO 4: Output Current Total Reuse Percentage
        st.divider()
        st.subheader("🎯 Current Total Reuse Percentage")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            reused_models = len(df[df['last_run_status'] == 'reused'])
            total = len(df)
            reuse_rate = (reused_models / total * 100) if total > 0 else 0
            
            st.metric(
                "Total Reuse Rate",
                f"{reuse_rate:.2f}%",
                delta=f"{reused_models:,} of {total:,} models"
            )
            
            # Progress bar
            st.progress(min(reuse_rate / 100, 1.0))
            
            if reuse_rate >= 30:
                st.success(f"✅ **Exceeds target of 30%** ({reuse_rate - 30:.1f}% above)")
            elif reuse_rate >= 20:
                st.info(f"ℹ️ **Good performance** (target is 30%)")
            else:
                st.warning(f"⚠️ **Below typical range** (target is 30%)")
        
        with col2:
            success_models = len(df[df['last_run_status'] == 'success'])
            success_rate = (success_models / total * 100) if total > 0 else 0
            
            st.metric(
                "Success Rate",
                f"{success_rate:.2f}%",
                delta=f"{success_models:,} models"
            )
        
        with col3:
            error_models = len(df[df['last_run_status'] == 'error'])
            error_rate = (error_models / total * 100) if total > 0 else 0
            
            st.metric(
                "Error Rate",
                f"{error_rate:.2f}%",
                delta=f"{error_models:,} models",
                delta_color="inverse"
            )
        
        # Insights
        st.divider()
        st.subheader("💡 Insights & Recommendations")
        
        insights = []
        
        if reuse_rate < 20:
            insights.append("🔸 **Low reuse rate**: Consider enabling incremental builds with freshness checks to improve cache utilization")
        
        if freshness_pct < 50:
            insights.append(f"🔸 **Low freshness coverage ({freshness_pct:.1f}%)**: Add freshness configuration to more models to enable reuse")
        
        if outside_slo > 0:
            insights.append(f"🔸 **{outside_slo} models outside SLO**: Review and update models that haven't run within their expected timeframe")
        
        if error_rate > 5:
            insights.append(f"🔸 **High error rate ({error_rate:.1f}%)**: Investigate and fix failing models")
        
        if reuse_rate >= 30 and error_rate < 5:
            insights.append("✅ **Excellent performance**: Your environment is well-configured with good reuse rates and low errors")
        
        if insights:
            for insight in insights:
                st.markdown(insight)
        else:
            st.success("✅ No major issues detected")
        
        # Download option
        st.divider()
        st.subheader("💾 Download Data")
        
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Full Analysis as CSV",
            data=csv,
            file_name=f"model_reuse_slo_analysis_env_{environment_id}.csv",
            mime="text/csv"
        )
        
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your API key and environment ID")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


def show_cost_analysis():
    """Show cost analysis and ROI tracking."""
    st.header("💰 Cost Analysis & ROI")
    st.markdown("Quantify the financial impact of your dbt optimization efforts and model reuse strategy")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Cost Configuration Section
    st.subheader("⚙️ Cost Configuration")
    
    with st.expander("📘 How Cost Calculation Works", expanded=False):
        st.markdown("""
        ### Cost Calculation Method
        
        **For models that execute (status = success)**:
        - `Cost = (Execution Time in seconds / 3600) × Warehouse Cost per Hour`
        
        **For reused models**:
        - **Actual Cost = $0** (they skip execution)
        - **Savings = Average execution time when model runs × Cost per Hour**
        
        ### Example
        ```
        Model: fct_orders
        - When it runs (status=success): Takes 300 seconds
        - Average run time: 300 seconds
        - Cost when it runs: (300/3600) × $4 = $0.33
        
        When reused:
        - Actual cost: $0.00
        - Savings: $0.33 (what we would have paid)
        ```
        
        ### Warehouse Costs (Typical Snowflake Pricing)
        Based on Snowflake on-demand pricing. Adjust based on your actual contract pricing.
        
        ### ROI Calculation
        - **ROI** = (Total Savings / Total Cost) × 100
        - Higher reuse rate = Higher ROI
        """)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        warehouse_size = st.selectbox(
            "Warehouse Size",
            options=["X-Small", "Small", "Medium", "Large", "X-Large", "2X-Large", "3X-Large", "4X-Large"],
            index=2,
            help="Select your typical warehouse size"
        )
    
    with col2:
        # Default costs per hour (Snowflake on-demand)
        default_costs = {
            "X-Small": 1.0,
            "Small": 2.0,
            "Medium": 4.0,
            "Large": 8.0,
            "X-Large": 16.0,
            "2X-Large": 32.0,
            "3X-Large": 64.0,
            "4X-Large": 128.0
        }
        
        cost_per_hour = st.number_input(
            "Cost per Hour ($)",
            min_value=0.0,
            value=float(default_costs[warehouse_size]),
            step=0.1,
            help="Cost per compute hour for your warehouse"
        )
    
    with col3:
        currency = st.selectbox(
            "Currency",
            options=["USD", "EUR", "GBP", "CAD", "AUD"],
            help="Display currency"
        )
    
    # Job source selection
    st.subheader("📋 Analysis Scope")
    
    col1, col2 = st.columns(2)
    
    with col1:
        job_source = st.selectbox(
            "Job Source",
            options=["All Jobs in Environment", "Specific Job ID"],
            help="Analyze all jobs or filter to one",
            key="cost_job_source"
        )
    
    with col2:
        job_types_filter = st.multiselect(
            "Job Types",
            options=["ci", "merge", "scheduled", "other"],
            default=["scheduled"],
            help="Filter by job type",
            key="cost_job_types"
        )
    
    col3, col4 = st.columns(2)
    
    with col3:
        if job_source == "Specific Job ID":
            job_id_input = st.text_input("Job ID", help="Specific dbt Cloud job ID", key="cost_job_id")
        else:
            job_id_input = None
            run_status_filter = st.multiselect(
                "Run Status",
                options=["Success", "Error", "Cancelled"],
                default=["Success"],
                help="Filter runs by completion status",
                key="cost_run_status"
            )
    
    with col4:
        # SAO filter option
        sao_only = st.checkbox(
            "SAO Jobs Only",
            value=True,
            help="Only analyze jobs with State-Aware Orchestration enabled (recommended for accurate cost/reuse metrics)",
            key="cost_sao_only"
        )
        if job_source == "Specific Job ID":
            run_status_filter = st.multiselect(
                "Run Status",
                options=["Success", "Error", "Cancelled"],
                default=["Success"],
                help="Filter runs by completion status",
                key="cost_run_status2"
            )
    
    # Date range for analysis
    st.subheader("📅 Analysis Period")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        default_start = datetime.now() - timedelta(days=30)
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            help="Start of analysis period"
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime.now(),
            help="End of analysis period"
        )
    
    with col3:
        max_runs = st.slider(
            "Max Runs",
            min_value=10,
            max_value=100,
            value=50,
            help="Number of runs to analyze"
        )
    
    analyze_button = st.button("💰 Calculate Costs & ROI", type="primary", key="cost_analyze")
    
    if not analyze_button:
        st.info("⬆️ Configure cost parameters and job source, then click 'Calculate Costs & ROI' to begin")
        
        # Show example output
        with st.expander("📊 What You'll See"):
            st.markdown("""
            ### Key Metrics
            - **Total Cost**: Total compute cost for the period
            - **Cost Savings**: Money saved from model reuse
            - **ROI**: Return on investment from freshness configs
            - **Cost per Run**: Average cost per job execution
            
            ### Visualizations
            - Cost trend over time
            - Cost by model (top 20 most expensive)
            - Savings from reuse over time
            - Cost breakdown by status (success vs reused)
            
            ### Detailed Analysis
            - Per-run cost breakdown
            - Model-level cost analysis
            - Reuse rate impact on costs
            """)
        return
    
    # Validation
    if job_source == "Specific Job ID" and not job_id_input:
        st.error("❌ Please provide a Job ID when using 'Specific Job ID' mode")
        return
    elif job_source == "All Jobs in Environment" and not config.get('environment_id'):
        st.error("❌ Please configure Environment ID in the Configuration tab to use 'All Jobs in Environment' mode")
        return
    
    # Convert status filter to status codes
    status_codes = []
    if "Success" in run_status_filter:
        status_codes.append(10)
    if "Error" in run_status_filter:
        status_codes.append(20)
    if "Cancelled" in run_status_filter:
        status_codes.append(30)
    
    if not status_codes:
        st.error("❌ Please select at least one run status to filter by")
        return
    
    try:
        # Convert dates to datetime
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text(f"🔄 Fetching runs from {start_date} to {end_date}...")
        
        # Fetch runs based on job source
        runs = []
        
        if job_source == "All Jobs in Environment":
            # Get all jobs in the environment
            jobs_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
            headers = {'Authorization': f'Token {config["api_key"]}'}
            params = {'environment_id': config['environment_id'], 'limit': 100}
            
            jobs_response = requests.get(jobs_url, headers=headers, params=params)
            jobs_response.raise_for_status()
            jobs = jobs_response.json().get('data', [])
            
            # Filter by job type
            jobs = filter_jobs_by_type(jobs, job_types_filter)
            
            if not jobs:
                st.error(f"❌ No jobs found matching job types: {', '.join(job_types_filter)}")
                progress_bar.empty()
                status_text.empty()
                return
            
            status_text.text(f"🔄 Found {len(jobs)} jobs. Fetching runs...")
            
            # Get runs from all filtered jobs
            all_runs = []
            for idx, job in enumerate(jobs):
                job_runs = get_job_runs(
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    str(job['id']),
                    config.get('environment_id'),
                    limit=max_runs,
                    status=status_codes
                )
                all_runs.extend(job_runs)
                
                # Update progress
                progress = int((idx + 1) / len(jobs) * 10)
                progress_bar.progress(progress)
            
            # Sort by date and limit
            all_runs = sorted(all_runs, key=lambda x: x.get('created_at', ''), reverse=True)[:max_runs]
            runs = all_runs
            
        else:  # Specific Job ID
            runs = get_job_runs(
                config['api_base'],
                config['api_key'],
                config['account_id'],
                job_id_input,
                config.get('environment_id'),
                limit=max_runs,
                status=status_codes
            )
        
        # Filter by date
        if start_datetime or end_datetime:
            filtered_runs = []
            for run in runs:
                created_at_str = run.get('created_at')
                if created_at_str:
                    try:
                        created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        created_at = created_at.replace(tzinfo=None)
                        
                        if start_datetime and created_at < start_datetime:
                            continue
                        if end_datetime and created_at > end_datetime:
                            continue
                        
                        filtered_runs.append(run)
                    except:
                        pass
            runs = filtered_runs
        
        if not runs:
            st.warning("No runs found in the specified date range")
            progress_bar.empty()
            status_text.empty()
            return
        
        # Filter by SAO if requested
        original_run_count = len(runs)
        if sao_only:
            status_text.text(f"🔍 Filtering to SAO-enabled jobs only...")
            sao_runs, non_sao_runs = filter_runs_by_sao(runs)
            runs = sao_runs
            
            if not runs:
                st.warning(f"⚠️ No runs found from SAO-enabled jobs. Found {len(non_sao_runs)} runs from non-SAO jobs.")
                st.info("💡 **Tip**: Uncheck 'SAO Jobs Only' to analyze all jobs, or enable SAO on your jobs for accurate cost/reuse metrics.")
                progress_bar.empty()
                status_text.empty()
                return
            
            if non_sao_runs:
                st.info(f"ℹ️ Filtered to {len(runs)} SAO-enabled runs (excluded {len(non_sao_runs)} non-SAO runs)")
        
        status_text.text(f"✅ Found {len(runs)} runs. Analyzing costs in parallel...")
        progress_bar.progress(10)
        
        # Analyze runs in parallel
        all_model_statuses = []
        completed_runs = 0
        failed_runs = []
        max_workers = min(10, len(runs))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_run = {
                executor.submit(
                    process_single_run,
                    config['api_base'],
                    config['api_key'],
                    config['account_id'],
                    run.get('id'),
                    run.get('created_at'),
                    run.get('job_definition_id'),
                    run.get('job', {}).get('name') if run.get('job') else None,
                    run.get('status')
                ): run.get('id') for run in runs
            }
            
            for future in as_completed(future_to_run):
                run_id = future_to_run[future]
                completed_runs += 1
                
                progress = 10 + int((completed_runs / len(runs)) * 80)
                progress_bar.progress(progress)
                status_text.text(f"🔄 Processed {completed_runs}/{len(runs)} runs...")
                
                try:
                    result = future.result()
                    if result['success']:
                        all_model_statuses.extend(result['models'])
                    else:
                        failed_runs.append((run_id, result.get('error', 'Unknown error')))
                except Exception as e:
                    failed_runs.append((run_id, str(e)))
        
        progress_bar.progress(100)
        status_text.text("✅ Analysis complete!")
        
        # Show warnings for failed runs
        if failed_runs:
            with st.expander(f"⚠️ {len(failed_runs)} run(s) failed to process", expanded=False):
                for run_id, error in failed_runs:
                    st.caption(f"Run {run_id}: {error}")
        
        # Convert to dataframe
        if all_model_statuses:
            df = pd.DataFrame(all_model_statuses)
        else:
            df = pd.DataFrame()
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        
        if df.empty:
            st.warning("No cost data found for the specified date range")
            return
        
        # Calculate average execution time per model when it actually runs (status = success)
        success_times = df[df['status'] == 'success'].groupby('unique_id')['execution_time'].mean().to_dict()
        
        # For each row, get the expected execution time
        # If success: use actual time
        # If reused: use average success time for that model
        df['expected_execution_time'] = df.apply(
            lambda row: row['execution_time'] if row['status'] == 'success' 
            else success_times.get(row['unique_id'], row['execution_time']),
            axis=1
        )
        
        # Calculate costs
        df['execution_time_hours'] = df['execution_time'] / 3600
        df['expected_time_hours'] = df['expected_execution_time'] / 3600
        
        # Actual cost (only for models that ran)
        df['cost'] = df.apply(
            lambda row: row['execution_time_hours'] * cost_per_hour if row['status'] == 'success'
            else 0,
            axis=1
        )
        
        # What the cost would have been if model ran (using expected time for reused models)
        df['cost_if_not_reused'] = df['expected_time_hours'] * cost_per_hour
        
        # Savings = what we would have paid - what we actually paid
        df['savings'] = df['cost_if_not_reused'] - df['cost']
        
        # Convert run_created_at to datetime
        df['run_created_at'] = pd.to_datetime(df['run_created_at'])
        
        st.success(f"✅ Analyzed {len(df):,} model executions across {df['run_id'].nunique()} runs")
        
        # KEY METRICS
        st.divider()
        st.subheader("💰 Key Financial Metrics")
        
        total_cost = df['cost'].sum()
        total_savings = df['savings'].sum()
        total_cost_if_not_reused = df['cost_if_not_reused'].sum()
        avg_cost_per_run = total_cost / df['run_id'].nunique() if df['run_id'].nunique() > 0 else 0
        roi = (total_savings / total_cost * 100) if total_cost > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Compute Cost",
                f"{currency} ${total_cost:,.2f}",
                help="Actual cost paid for compute"
            )
        
        with col2:
            st.metric(
                "Cost Savings from Reuse",
                f"{currency} ${total_savings:,.2f}",
                delta=f"{(total_savings/total_cost_if_not_reused*100):.1f}% saved",
                help="Money saved by reusing cached models"
            )
        
        with col3:
            st.metric(
                "Avg Cost per Run",
                f"{currency} ${avg_cost_per_run:,.2f}",
                help="Average cost per job execution"
            )
        
        with col4:
            st.metric(
                "ROI from Reuse",
                f"{roi:.1f}%",
                help="Return on investment from model reuse"
            )
        
        # Cost without reuse
        st.info(f"💡 **Without reuse, you would have spent**: {currency} ${total_cost_if_not_reused:,.2f} (saved {currency} ${total_savings:,.2f}!)")
        
        # COST TREND OVER TIME
        st.divider()
        st.subheader("📈 Cost Trends Over Time")
        
        # Aggregate by run
        run_costs = df.groupby(['run_id', 'run_created_at']).agg({
            'cost': 'sum',
            'savings': 'sum',
            'cost_if_not_reused': 'sum'
        }).reset_index()
        
        run_costs = run_costs.sort_values('run_created_at')
        
        # Create figure with dual axis
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=run_costs['run_created_at'],
            y=run_costs['cost'],
            name='Actual Cost',
            mode='lines+markers',
            line=dict(color='red', width=2),
            fill='tozeroy'
        ))
        
        fig.add_trace(go.Scatter(
            x=run_costs['run_created_at'],
            y=run_costs['cost_if_not_reused'],
            name='Cost Without Reuse',
            mode='lines',
            line=dict(color='lightgray', width=2, dash='dash')
        ))
        
        fig.add_trace(go.Scatter(
            x=run_costs['run_created_at'],
            y=run_costs['savings'],
            name='Savings',
            mode='lines+markers',
            line=dict(color='green', width=2),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title=f"Cost Trends: {warehouse_size} Warehouse @ ${cost_per_hour}/hour",
            xaxis_title="Date",
            yaxis_title=f"Cost ({currency})",
            yaxis2=dict(
                title=f"Savings ({currency})",
                overlaying='y',
                side='right'
            ),
            hovermode='x unified',
            height=500,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # TOP 20 MOST EXPENSIVE MODELS
        st.divider()
        st.subheader("💸 Most Expensive Models")
        
        model_costs = df.groupby('unique_id').agg({
            'cost': 'sum',
            'execution_time': 'sum',
            'status': 'count'
        }).reset_index()
        
        model_costs.columns = ['Model', 'Total Cost', 'Total Time (s)', 'Executions']
        model_costs = model_costs.sort_values('Total Cost', ascending=False).head(20)
        
        fig_top = px.bar(
            model_costs,
            x='Total Cost',
            y='Model',
            orientation='h',
            title='Top 20 Most Expensive Models',
            labels={'Total Cost': f'Total Cost ({currency})', 'Model': 'Model'},
            color='Total Cost',
            color_continuous_scale='Reds'
        )
        
        fig_top.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig_top, use_container_width=True)
        
        # DETAILED RUN BREAKDOWN
        st.divider()
        st.subheader("📋 Detailed Run Breakdown")
        
        run_summary = df.groupby(['run_id', 'run_created_at']).agg({
            'cost': 'sum',
            'savings': 'sum',
            'cost_if_not_reused': 'sum',
            'unique_id': 'count'
        }).reset_index()
        
        run_summary.columns = ['Run ID', 'Date', 'Actual Cost', 'Savings', 'Cost Without Reuse', 'Models']
        run_summary['Date'] = pd.to_datetime(run_summary['Date']).dt.strftime('%Y-%m-%d %H:%M')
        run_summary['Reuse %'] = ((run_summary['Savings'] / run_summary['Cost Without Reuse']) * 100).round(1)
        
        # Format currency columns
        for col in ['Actual Cost', 'Savings', 'Cost Without Reuse']:
            run_summary[col] = run_summary[col].apply(lambda x: f"${x:,.2f}")
        
        st.dataframe(
            run_summary,
            width='stretch',
            hide_index=True
        )
        
        # MODEL-LEVEL COST ANALYSIS
        st.divider()
        st.subheader("🔍 Model-Level Cost Analysis")
        
        model_detail = df.groupby('unique_id').agg({
            'cost': 'sum',
            'savings': 'sum',
            'execution_time': ['sum', 'mean', 'count'],
            'status': lambda x: x.value_counts().to_dict()
        }).reset_index()
        
        model_detail.columns = ['Model', 'Total Cost', 'Total Savings', 'Total Time (s)', 'Avg Time (s)', 'Executions', 'Status Breakdown']
        model_detail = model_detail.sort_values('Total Cost', ascending=False)
        
        # Format
        model_detail['Total Cost'] = model_detail['Total Cost'].apply(lambda x: f"${x:,.2f}")
        model_detail['Total Savings'] = model_detail['Total Savings'].apply(lambda x: f"${x:,.2f}")
        model_detail['Total Time (s)'] = model_detail['Total Time (s)'].round(2)
        model_detail['Avg Time (s)'] = model_detail['Avg Time (s)'].round(2)
        
        st.dataframe(
            model_detail,
            width='stretch',
            hide_index=True
        )
        
        # DOWNLOAD OPTION
        st.divider()
        st.subheader("💾 Download Cost Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Download run summary
            csv_runs = run_summary.to_csv(index=False)
            st.download_button(
                label="📥 Download Run Summary CSV",
                data=csv_runs,
                file_name=f"cost_analysis_runs_{start_date}_to_{end_date}.csv",
                mime="text/csv"
            )
        
        with col2:
            # Download model details
            csv_models = df.to_csv(index=False)
            st.download_button(
                label="📥 Download Full Model Data CSV",
                data=csv_models,
                file_name=f"cost_analysis_models_{start_date}_to_{end_date}.csv",
                mime="text/csv"
            )
        
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your credentials and IDs")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


def show_job_overlap_analysis():
    """Show job overlap analysis to identify models run in multiple jobs."""
    st.header("🔀 Job Overlap Analysis")
    st.markdown("**Identify unnecessary model duplication** across jobs in your environment (pre-SAO optimization)")
    
    # Check if configured
    if not st.session_state.config['configured']:
        st.warning("⚠️ Please configure your settings in the Configuration tab first")
        return
    
    config = st.session_state.config
    
    # Info about what this does
    with st.expander("ℹ️ What is Job Overlap Analysis?", expanded=False):
        st.markdown("""
        ### The Problem
        In environments **without State-Aware Orchestration (SAO)**, multiple jobs may independently run the same models, wasting compute resources.
        
        ### Example
        ```
        Job A: Runs models X, Y, Z
        Job B: Runs models Y, Z, W
        Job C: Runs models Z, W, Q
        
        Problem: Models Y, Z, W are executed multiple times!
        ```
        
        ### Solution
        This analysis helps you:
        1. **Identify overlapping models** across jobs
        2. **Quantify waste** (how many redundant executions)
        3. **Consolidate jobs** to eliminate duplication
        4. **Prioritize SAO adoption** to automatically handle this
        
        ### When to Use
        - Before implementing SAO
        - When consolidating jobs
        - When reducing compute costs
        - When auditing job efficiency
        """)
    
    st.divider()
    
    # Environment selection
    col1, col2 = st.columns([2, 2])
    
    with col1:
        environment_id_input = st.text_input(
            "Environment ID (Optional)",
            value=config.get('environment_id', ''),
            help="Leave blank to analyze all jobs in the account, or specify environment ID to filter",
            key="overlap_environment_id"
        )
    
    with col2:
        job_types_filter = st.multiselect(
            "Filter Job Types",
            options=["ci", "merge", "scheduled", "other"],
            default=["scheduled"],
            help="Which job types to analyze",
            key="overlap_job_types"
        )
    
    analyze_button = st.button("🔍 Analyze Job Overlap", type="primary", key="overlap_analyze", width='stretch')
    
    if not analyze_button:
        st.info("⬆️ Click 'Analyze Job Overlap' to identify models run in multiple jobs")
        
        with st.expander("📊 What You'll See"):
            st.markdown("""
            ### Key Metrics
            - **Total Jobs Analyzed**: Number of jobs successfully analyzed
            - **Jobs Skipped**: Jobs excluded from analysis (currently running or never succeeded)
            - **Total Models**: Unique models found
            - **Overlapping Models**: Models run in multiple jobs
            - **Overlap Rate**: % of models with duplication
            
            ### What Gets Skipped
            Jobs are automatically excluded if:
            - Currently running (Queued/Starting/Running status)
            - Never completed successfully (no successful runs in history)
            - Missing artifacts (run_results.json not available)
            
            ### Visualizations
            - Bar chart of most duplicated models
            - Table showing which jobs run which models
            - Recommendations for consolidation
            
            ### Export
            - Download detailed overlap report
            - Model-to-jobs mapping
            - Job-to-models mapping
            """)
        return
    
    # Fetch and analyze jobs
    try:
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Fetch all jobs
        status_text.text("🔄 Fetching jobs...")
        url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/jobs/'
        headers = {'Authorization': f'Token {config["api_key"]}'}
        
        params = {'limit': 100}
        if environment_id_input:
            params['environment_id'] = environment_id_input
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        jobs = response.json().get('data', [])
        
        # Filter by job type
        jobs = filter_jobs_by_type(jobs, job_types_filter)
        
        if not jobs:
            st.warning(f"No jobs found matching the selected job types: {', '.join(job_types_filter)}")
            progress_bar.empty()
            status_text.empty()
            return
        
        st.success(f"✅ Found {len(jobs)} jobs to analyze (filtered by: {', '.join(job_types_filter)})")
        progress_bar.progress(10)
        
        # Analyze each job
        status_text.text(f"🔄 Analyzing jobs...")
        
        model_to_jobs = defaultdict(list)
        job_to_models = {}
        jobs_analyzed = 0
        jobs_skipped = 0
        jobs_running = 0
        jobs_never_succeeded = 0
        
        for idx, job in enumerate(jobs):
            job_id = job['id']
            job_name = job['name']
            
            progress = 10 + int((idx / len(jobs)) * 80)
            progress_bar.progress(progress)
            status_text.text(f"🔄 Analyzing job {idx + 1}/{len(jobs)}: {job_name[:50]}...")
            
            try:
                # First, check if job has any runs at all and their status
                check_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/'
                check_params = {
                    'job_definition_id': job_id,
                    'limit': 1,
                    'order_by': '-id'  # Get most recent run regardless of status
                }
                
                check_response = requests.get(check_url, headers=headers, params=check_params)
                check_response.raise_for_status()
                recent_runs = check_response.json().get('data', [])
                
                # Check if most recent run is currently running
                # If so, we'll fetch the latest successful run instead of skipping
                is_currently_running = recent_runs and recent_runs[0].get('status') in [1, 2, 3]  # Queued, Starting, Running
                if is_currently_running:
                    jobs_running += 1
                
                # Now get latest successful run
                run_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/'
                run_params = {
                    'job_definition_id': job_id,
                    'limit': 1,
                    'order_by': '-id',
                    'status': '10'  # Success
                }
                
                run_response = requests.get(run_url, headers=headers, params=run_params)
                run_response.raise_for_status()
                runs = run_response.json().get('data', [])
                
                if not runs:
                    # No successful runs found
                    jobs_never_succeeded += 1
                    jobs_skipped += 1
                    continue
                
                run_id = runs[0]['id']
                
                # Use the new step-based approach to get accurate model lists
                # Fetch run steps and filter to only run/build commands
                run_details_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/{run_id}/'
                run_details_params = {'include_related': '["run_steps"]'}
                
                run_details_response = requests.get(run_details_url, headers=headers, params=run_details_params)
                run_details_response.raise_for_status()
                
                run_data = run_details_response.json().get('data', {})
                run_steps = run_data.get('run_steps', [])
                
                # Filter to only 'dbt run' and 'dbt build' commands
                relevant_steps = []
                for step in run_steps:
                    step_name = step.get('name', '')
                    step_index = step.get('index')
                    
                    if 'dbt run' in step_name or 'dbt build' in step_name:
                        relevant_steps.append(step_index)
                
                # Collect models from all relevant steps
                all_models = set()  # Use set to avoid duplicates
                
                if relevant_steps:
                    # Fetch run_results.json from each relevant step
                    for step_idx in relevant_steps:
                        artifacts_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/{run_id}/artifacts/run_results.json?step={step_idx}'
                        
                        try:
                            results_response = requests.get(artifacts_url, headers=headers)
                            if results_response.ok:
                                run_results = results_response.json()
                                
                                # Extract models from this step
                                for result in run_results.get('results', []):
                                    unique_id = result.get('unique_id', '')
                                    if unique_id.startswith('model.'):
                                        all_models.add(unique_id)
                        except Exception:
                            # If this step fails, continue to next step
                            continue
                else:
                    # Fallback: no relevant steps found, try default run_results.json
                    artifacts_url = f'{config["api_base"]}/api/v2/accounts/{config["account_id"]}/runs/{run_id}/artifacts/run_results.json'
                    
                    try:
                        results_response = requests.get(artifacts_url, headers=headers)
                        if results_response.ok:
                            run_results = results_response.json()
                            
                            for result in run_results.get('results', []):
                                unique_id = result.get('unique_id', '')
                                if unique_id.startswith('model.'):
                                    all_models.add(unique_id)
                    except Exception:
                        pass
                
                if not all_models:
                    jobs_skipped += 1
                    continue
                
                # Convert set to list
                models = list(all_models)
                
                # Store mappings
                job_to_models[job_name] = {
                    'job_id': job_id,
                    'run_id': run_id,
                    'models': models
                }
                
                for model in models:
                    model_to_jobs[model].append({
                        'job_name': job_name,
                        'job_id': job_id,
                        'run_id': run_id
                    })
                
                jobs_analyzed += 1
                
            except Exception as e:
                jobs_skipped += 1
                continue
        
        progress_bar.progress(100)
        status_text.text("✅ Analysis complete!")
        
        # Clear progress indicators
        progress_bar.empty()
        status_text.empty()
        
        # Find overlapping models
        overlapping_models = {
            model: jobs_list
            for model, jobs_list in model_to_jobs.items()
            if len(jobs_list) > 1
        }
        
        # Display results
        st.divider()
        st.subheader("📊 Analysis Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Jobs Analyzed", f"{jobs_analyzed:,}")
            if jobs_running > 0:
                st.caption(f"ℹ️ {jobs_running} job(s) currently running - using latest successful run")
            if jobs_skipped > 0:
                skip_details = []
                if jobs_never_succeeded > 0:
                    skip_details.append(f"{jobs_never_succeeded} never succeeded")
                other_skipped = jobs_skipped - jobs_never_succeeded
                if other_skipped > 0:
                    skip_details.append(f"{other_skipped} other")
                
                if skip_details:
                    st.caption(f"⚠️ {jobs_skipped} jobs skipped: {', '.join(skip_details)}")
        
        with col2:
            st.metric("Total Unique Models", f"{len(model_to_jobs):,}")
        
        with col3:
            st.metric("Overlapping Models", f"{len(overlapping_models):,}")
        
        with col4:
            overlap_rate = (len(overlapping_models) / len(model_to_jobs) * 100) if len(model_to_jobs) > 0 else 0
            st.metric("Overlap Rate", f"{overlap_rate:.1f}%")
        
        # Show info about currently running and skipped jobs
        info_messages = []
        
        if jobs_running > 0:
            info_messages.append(f'- **{jobs_running} job(s) currently running** - Using their latest successful run for analysis')
        
        if jobs_skipped > 0:
            skip_info = []
            if jobs_never_succeeded > 0:
                skip_info.append(f'- **{jobs_never_succeeded} never succeeded** - No successful runs in history (may be new, disabled, or have errors)')
            other_skipped = jobs_skipped - jobs_never_succeeded
            if other_skipped > 0:
                skip_info.append(f'- **{other_skipped} other** - Missing artifacts or other issues')
            
            if skip_info:
                info_messages.append(f'**{jobs_skipped} job(s) were excluded:**')
                info_messages.extend(skip_info)
        
        if info_messages:
            st.divider()
            st.info('ℹ️ ' + '\n'.join(info_messages))
        
        # Assessment
        if len(overlapping_models) == 0:
            st.success("✅ **Excellent!** No models are being run in multiple jobs. Your job structure is optimized!")
            
            # Show table of all jobs analyzed even when no overlap
            st.divider()
            st.subheader("📊 Jobs Analyzed")
            
            # Create summary of all jobs
            jobs_summary = []
            for job_name, job_data in job_to_models.items():
                jobs_summary.append({
                    'Job Name': job_name,
                    'Job ID': job_data['job_id'],
                    'Run ID': job_data['run_id'],
                    'Model Count': len(job_data['models'])
                })
            
            jobs_summary_df = pd.DataFrame(jobs_summary)
            jobs_summary_df = jobs_summary_df.sort_values('Model Count', ascending=False)
            
            st.dataframe(
                jobs_summary_df,
                width='stretch',
                hide_index=True
            )
            
            st.caption(f"✓ Analyzed {len(jobs_summary_df)} jobs with a total of {len(model_to_jobs)} unique models")
            
        elif overlap_rate < 10:
            st.info(f"✓ **Good** - Only {overlap_rate:.1f}% of models have overlap. Minor optimization opportunity.")
        elif overlap_rate < 25:
            st.warning(f"⚠️ **Moderate Waste** - {overlap_rate:.1f}% of models are duplicated. Consider job consolidation.")
        else:
            st.error(f"❌ **High Waste** - {overlap_rate:.1f}% of models are duplicated! Significant optimization opportunity.")
        
        if len(overlapping_models) > 0:
            # Most duplicated models
            st.divider()
            st.subheader("🔝 Most Duplicated Models")
            
            # Sort by number of jobs
            sorted_overlaps = sorted(
                overlapping_models.items(),
                key=lambda x: len(x[1]),
                reverse=True
            )[:20]  # Top 20
            
            # Create dataframe for chart
            overlap_data = []
            for model_id, jobs_list in sorted_overlaps:
                model_name = model_id.split('.')[-1]
                overlap_data.append({
                    'Model': model_name,
                    'Full ID': model_id,
                    'Job Count': len(jobs_list),
                    'Jobs': ', '.join([j['job_name'] for j in jobs_list])
                })
            
            overlap_df = pd.DataFrame(overlap_data)
            
            # Bar chart
            fig_overlap = px.bar(
                overlap_df.head(20),
                x='Job Count',
                y='Model',
                orientation='h',
                title='Top 20 Models by Number of Jobs Running Them',
                text='Job Count',
                color='Job Count',
                color_continuous_scale='Reds'
            )
            
            fig_overlap.update_layout(height=600, showlegend=False)
            st.plotly_chart(fig_overlap, use_container_width=True)
            
            # Detailed table
            st.divider()
            st.subheader("📋 Overlap Details")
            
            # Expandable sections for each overlapping model
            for model_id, jobs_list in sorted_overlaps[:10]:  # Show top 10 in detail
                model_name = model_id.split('.')[-1]
                
                with st.expander(f"🔸 {model_name} (in {len(jobs_list)} jobs)"):
                    st.code(model_id, language=None)
                    st.markdown("**Found in these jobs:**")
                    
                    for job_info in jobs_list:
                        st.markdown(f"- **{job_info['job_name']}** (Job ID: {job_info['job_id']}, Run ID: {job_info['run_id']})")
                    
                    st.warning(f"💡 **Recommendation**: This model is executed {len(jobs_list)} times. Consider consolidating to a single job or implementing SAO.")
            
            # Full overlap table
            st.divider()
            st.subheader("📊 Complete Overlap Report")
            
            st.dataframe(
                overlap_df[['Model', 'Job Count', 'Jobs']],
                width='stretch',
                hide_index=True
            )
            
            # Waste calculation
            st.divider()
            st.subheader("💸 Waste Calculation")
            
            total_redundant_executions = sum(len(jobs) - 1 for jobs in overlapping_models.values())
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric(
                    "Redundant Executions",
                    f"{total_redundant_executions:,}",
                    help="Number of unnecessary model executions (executions beyond the first)"
                )
                
                st.caption("These executions could be eliminated through job consolidation or SAO")
            
            with col2:
                if jobs_analyzed > 0:
                    avg_redundancy = total_redundant_executions / jobs_analyzed
                    st.metric(
                        "Avg Redundancy per Job",
                        f"{avg_redundancy:.1f}",
                        help="Average number of duplicate models per job"
                    )
            
            # Recommendations
            st.divider()
            st.subheader("💡 Recommendations")
            
            if overlap_rate > 50:
                st.markdown("""
                ### 🚨 High Priority Actions
                1. **Implement State-Aware Orchestration (SAO)** - Automates model reuse across jobs
                2. **Consolidate jobs** - Merge jobs with heavy overlap
                3. **Review job design** - Consider if all jobs are necessary
                """)
            elif overlap_rate > 25:
                st.markdown("""
                ### ⚠️ Medium Priority Actions
                1. **Plan for SAO adoption** - Will automatically eliminate this waste
                2. **Consolidate high-overlap jobs** - Focus on top duplicated models
                3. **Document job purposes** - Ensure each job has a clear, unique purpose
                """)
            else:
                st.markdown("""
                ### ✓ Low Priority Actions
                1. **Monitor over time** - Track if overlap increases
                2. **Consider SAO** for automatic optimization
                3. **Document current structure** - Good baseline for future changes
                """)
            
            # Job-to-job overlap matrix (for smaller sets)
            if jobs_analyzed <= 20:
                st.divider()
                st.subheader("🔀 Job-to-Job Overlap Matrix")
                
                # Create matrix showing overlap between jobs
                job_names = list(job_to_models.keys())
                overlap_matrix = []
                
                for job1 in job_names:
                    row = {'Job': job1}
                    models1 = set(job_to_models[job1]['models'])
                    
                    for job2 in job_names:
                        if job1 == job2:
                            row[job2] = len(models1)
                        else:
                            models2 = set(job_to_models[job2]['models'])
                            overlap = len(models1.intersection(models2))
                            row[job2] = overlap  # Use 0 instead of empty string for consistent type
                    
                    overlap_matrix.append(row)
                
                matrix_df = pd.DataFrame(overlap_matrix)
                st.dataframe(matrix_df, width='stretch', hide_index=True)
                st.caption("Numbers show how many models are shared between jobs. Diagonal shows total models in each job.")
        
        # Download section
        st.divider()
        st.subheader("💾 Download Analysis Results")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Model to jobs mapping
            model_to_jobs_json = json.dumps(dict(model_to_jobs), indent=2, default=str)
            st.download_button(
                label="📥 Model-to-Jobs Mapping",
                data=model_to_jobs_json,
                file_name=f"model_to_jobs_{config['account_id']}.json",
                mime="application/json"
            )
        
        with col2:
            # Job to models mapping
            job_to_models_json = json.dumps(job_to_models, indent=2, default=str)
            st.download_button(
                label="📥 Job-to-Models Mapping",
                data=job_to_models_json,
                file_name=f"job_to_models_{config['account_id']}.json",
                mime="application/json"
            )
        
        with col3:
            # Overlapping models only
            if overlapping_models:
                overlapping_json = json.dumps(overlapping_models, indent=2, default=str)
                st.download_button(
                    label="📥 Overlapping Models Report",
                    data=overlapping_json,
                    file_name=f"overlapping_models_{config['account_id']}.json",
                    mime="application/json"
                )
        
    except requests.exceptions.HTTPError as e:
        st.error(f"❌ API Error: {e}")
        st.error("Please check your credentials and permissions")
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")
        st.exception(e)


if __name__ == "__main__":
    main()

