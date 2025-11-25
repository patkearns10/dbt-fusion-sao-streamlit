# Changelog

## [2.7.0] - November 25, 2025

### Major Features - SAO Detection & Filtering 🎯

#### State-Aware Orchestration (SAO) Filtering
- **NEW: Automatic SAO detection** from job configuration
- **NEW: "SAO Jobs Only" filter** in Historical Trends and Cost Analysis tabs
- **Smart filtering** excludes jobs without SAO for accurate reuse metrics
- **Default: Enabled** - filters to SAO-only by default (best practice)

#### Key Benefits
- ✅ **Accurate reuse metrics** - No more 0% reuse from non-SAO jobs
- ✅ **Meaningful cost savings** - Only shows real SAO financial impact
- ✅ **Better trends** - Track true SAO performance over time
- ✅ **Clear feedback** - Shows exactly what was filtered and why

#### Technical Implementation
- `check_job_has_sao()` - Detects SAO from `cost_optimization_features` field
- `filter_runs_by_sao()` - Separates SAO and non-SAO runs
- Integrated into existing filtering pipeline (date → job type → SAO)
- Zero performance impact (uses existing API data)

#### User Interface
- **Historical Trends**: "SAO Jobs Only" checkbox (default: checked)
- **Cost Analysis**: "SAO Jobs Only" checkbox (default: checked)
- **Info messages**: Shows count of filtered runs
- **Warnings**: Alerts if no SAO jobs found with helpful guidance

#### Use Cases Enabled
1. **Pure SAO environments** - Clean, accurate metrics
2. **Mixed environments** - Focus on SAO performance
3. **Migration analysis** - Compare before/after SAO adoption
4. **Troubleshooting** - Identify jobs missing SAO

### Bug Fixes & Improvements

#### Run Status Detection Enhancements
- **Improved fallback detection** for reused models
  - Models with execution_time < 0.1s automatically marked as "reused"
  - Only applies when log parsing succeeds but model not in logs
  - Addresses issue where all models showed as "success" incorrectly
- **Enhanced diagnostics** in log processing
  - Shows reuse detection statistics
  - Reports average and median execution times
  - Helps identify when detection fails
- **Data quality warnings** in UI
  - Automatically detects suspicious runs (95%+ success, median < 0.05s)
  - Warns users about potential log parsing failures
  - Explains that counts are still accurate

#### Job Overlap Analysis Improvements
- **Proactive filtering** of currently running jobs
  - Checks job status before attempting analysis
  - Prevents errors from jobs with no successful run
  - Status codes: 1=Queued, 2=Starting, 3=Running
- **Categorized skip tracking**
  - Separately counts: running, never succeeded, other
  - Detailed breakdown in metrics display
  - Clear explanation of why jobs excluded
- **Enhanced user feedback**
  - Info box showing skip categories and counts
  - Actionable guidance (e.g., "re-run later")
  - Updated documentation explaining exclusions

## [2.6.0] - November 25, 2025

### Major Enhancements - Environment-First Workflow 🎯

#### Configuration Page Reorganization
- **Removed Job ID from main configuration** - No longer required for core workflows
- **Reorganized layout**: Configuration inputs at top, tab descriptions below
- **Streamlined fields**: 
  - Credentials: API URL, API Key, Account ID
  - Environment Settings: Environment ID, Project ID (optional)
- **Improved help text**: Better guidance on what each field is for
- **Summary first, details below**: Configuration inputs take priority

#### 📋 Model Details - Environment-First Approach
- **NEW: Environment (Latest) mode** - Get latest run from environment automatically
  - Fetches all jobs in environment
  - Filters by selected job types
  - Uses latest successful run across filtered jobs
- **Flexible Source Selection**:
  - Environment (Latest): Analyze most recent run from environment
  - Specific Job ID: Get latest run from a particular job
  - Specific Run ID: Analyze a specific run directly
- **NEW: Job Type Filtering**
  - Filter to ci, merge, scheduled, or other job types
  - Default: scheduled jobs only
  - Applies to Environment mode

#### 📈 Historical Trends - Environment-Wide Analysis
- **NEW: All Jobs in Environment mode** - Analyze all jobs across time range
  - Fetches runs from all jobs in environment
  - Filters by job types (ci, merge, scheduled, other)
  - Aggregates data across entire environment
- **Flexible Analysis**:
  - All Jobs in Environment: Environment-wide trends
  - Specific Job ID: Single job analysis (legacy mode)
- **Job Type Filtering**: Filter which job types to analyze
- **Better performance tracking**: See trends across your entire environment

#### 🔀 Job Overlap Analysis - Job Type Filtering
- **NEW: Job type filtering**
  - Filter overlap analysis by job types
  - Default: scheduled jobs only
  - Focus on production jobs or analyze all types
- **Improved messaging**: Shows which job types were analyzed
- **Better targeting**: Focus on specific job categories

### Technical Improvements
- **NEW: Helper functions**:
  - `determine_job_type(triggers)`: Detect job type from trigger config
  - `filter_jobs_by_type(jobs, job_types)`: Filter job lists by type
- **Job Type Detection Logic**:
  - `scheduled`: Has schedule trigger
  - `ci`: Has webhook trigger + custom_branch_only=True
  - `merge`: Has webhook trigger + custom_branch_only=False
  - `other`: Everything else
- **Session state cleanup**: Removed `job_id` from default config
- **Error handling**: Better validation for environment vs job ID modes
- **API efficiency**: Smarter fetching based on source mode

### Breaking Changes
- **Job ID removed from Configuration page**: Use per-tab job specification instead
- **Environment ID now primary**: Required for Environment-wide features
- **Job type filtering**: Default changed to "scheduled" only (was "all")

### Migration Guide
- **Before**: Set Job ID in Configuration → use for all tabs
- **After**: Set Environment ID in Configuration → specify job/run per tab as needed
- **Benefit**: More flexible, supports environment-wide analysis, clearer workflow

## [2.5.0] - November 24, 2025

### New Features 🎉

#### 🔀 Job Overlap Analysis Tab (NEW!)
- **Pre-SAO Optimization**: Identify models being run by multiple jobs
- **Job Inventory**: Analyze all jobs in environment
- **Overlap Detection**: Find models executed in multiple jobs
- **Overlap Ranking**: Sort by most duplicated models (bar chart)
- **Waste Calculation**: Quantify redundant executions
- **Job-to-Job Matrix**: Visual overlap matrix (for smaller job sets)
- **Detailed Reports**: Expandable sections for each overlapping model
- **Recommendations**: Priority-based suggestions for consolidation
- **Export Options**: Download mappings and overlap reports as JSON

#### Key Capabilities
- Identifies unnecessary model duplication across jobs
- Helps prioritize job consolidation efforts
- Quantifies compute waste before SAO adoption
- Provides actionable recommendations based on overlap severity

#### Use Cases
- **Pre-SAO environments**: Find redundant model executions
- **Job consolidation**: Identify which jobs to merge
- **Cost optimization**: Eliminate unnecessary compute
- **Audit efficiency**: Validate job structure

## [2.4.1] - November 24, 2025

### Bug Fixes & Improvements

#### 💰 Cost Analysis - Corrected Calculation Logic
- **FIXED: Savings calculation now accurate**
  - For reused models, uses average execution time from successful runs
  - Previously used reused time (~0s) which was incorrect
  - Now correctly calculates: "What would this model have cost if it ran?"
- **Removed: Savings Breakdown by Status** (redundant visualization)
- **Improved: Cost Distribution** now includes execution counts and percentages
- **Updated: Documentation** to reflect correct calculation methodology

#### How It Works Now
```python
# When a model runs (status=success):
cost = (execution_time / 3600) × cost_per_hour

# When a model is reused:
average_success_time = mean(all successful runs for this model)
cost = $0 (didn't execute)
savings = (average_success_time / 3600) × cost_per_hour
```

## [2.4.0] - November 24, 2025

### New Features 🎉

#### 💰 Cost Analysis Tab (NEW!)
- **Cost Estimation**: Calculate compute costs based on execution time and warehouse type
- **Savings Calculation**: Show money saved from model reuse
- **ROI Analysis**: Measure return on investment from freshness configurations
- **Cost Trends**: Visualize cost patterns over time
- **Top Expensive Models**: Identify the 20 most costly models
- **Model-Level Analysis**: Detailed cost breakdown per model
- **Warehouse Configuration**: Support for 8 warehouse sizes with customizable costs
- **Multi-Currency**: Support for USD, EUR, GBP, CAD, AUD
- **Export Options**: Download run summaries and model-level cost data

#### 📈 Historical Trends Enhancements
- **Reuse Rate Trending**: Track reuse percentage over time with trend line
- **Goal Visualization**: 30% reuse goal line on charts
- **Trend Statistics**: Average, peak, and lowest reuse rates
- **Trend Direction**: Calculate if reuse is improving or declining
- **Better Metrics**: Recent vs older period comparison

### Features
- Added 5th tab: "💰 Cost Analysis"
- Parallel processing for cost analysis (same as Historical Trends)
- Interactive cost trend charts with dual axes (cost + savings)
- Warehouse cost defaults based on Snowflake pricing
- Configurable cost per hour for custom pricing
- Date range filtering for cost analysis
- Run-level and model-level cost breakdowns

### Technical Improvements
- Enhanced `show_run_status_analysis()` with reuse rate trending
- New `show_cost_analysis()` function with comprehensive financial analysis
- Cost calculations: `(execution_time / 3600) × cost_per_hour`
- Savings calculations: reused models cost $0
- ROI formula: `(savings / total_cost) × 100`

## [2.3.0] - November 24, 2025

### Major Reorganization 🎯
- **Reduced from 5 tabs to 4** for clearer navigation
- **Environment Overview is now the primary dashboard** (Tab 1)
- **Removed Summary Statistics tab** (functionality merged into Environment Overview)
- **Renamed tabs** for clarity:
  - "Model Reuse & SLO Analysis" → "Environment Overview"
  - "Freshness Details" → "Model Details"
  - "Run Status Details" → "Historical Trends"

### Package Filtering ✅
- **NEW: `filter_to_main_project()` function**
  - Filters out 8 common dbt packages (dbt_project_evaluator, dbt_artifacts, etc.)
  - Ensures consistent model counts across all tabs
  - Focus on models you actually control
- Applied to all tabs for consistency
- **Fixes model count discrepancies**:
  - Before: Environment Overview (1620) vs Others (~2548 with packages)
  - After: All tabs show consistent counts for main project only

### UX Improvements
- **Enhanced Configuration tab**
  - New expandable "About the Analysis Tabs" section
  - Comprehensive descriptions of each tab's purpose
  - Better guidance for new users
  - Clearer data source information (real-time vs point-in-time)
- **Better messaging** throughout the app
- **Improved tooltips** and help text

### Documentation
- NEW: `REORGANIZATION_SUMMARY.md` - Complete reorganization guide
- Includes future enhancement suggestions
- User feedback mechanisms
- Success metrics

## [2.2.0] - November 24, 2025

### Performance Improvements
- **⚡ Parallel Processing for Run Status Analysis**
  - Up to 10 runs processed simultaneously using ThreadPoolExecutor
  - **5-10x faster** analysis times
  - 20 runs now take ~10-15 seconds (vs 40-100 seconds before)
  - Real-time progress tracking for parallel execution
  - Improved error handling with failed run collection

### UX Improvements
- **Configuration Tab Now First**
  - Moved Configuration tab to first position for better onboarding
  - Added expandable "About the Analysis Tabs" section
  - Comprehensive descriptions of all tabs with use cases
  - Helpful tips when configuration is saved
  - Better warnings when not configured

### Bug Fixes
- Fixed timezone issue in Model Reuse & SLO Analysis tab
  - Resolved "Cannot subtract tz-naive and tz-aware datetime-like objects" error
  - Properly handles UTC timestamps from GraphQL API
- Fixed Streamlit deprecation warnings
  - Replaced `use_container_width` with `width` parameter for dataframes and buttons
  - Updated all 16 instances throughout the app
  - Note: `st.plotly_chart` still uses `use_container_width=True` (not `width`)
  - Now compatible with Streamlit 2025+ API

### Updated Recommendations
- Increased recommended runs for analysis from 5-10 to 20-50
- Updated date ranges from 7 days to 14-30 days
- Revised performance expectations in documentation

## [2.1.0] - November 24, 2025

### Added
- **New Tab: Model Reuse & SLO Analysis** 🔄
  - GraphQL-based environment-wide analysis
  - Real-time model execution status and reuse tracking
  - SLO compliance table with filtering capabilities
  - Interactive visualizations:
    - Model distribution by build_after configuration (bar & pie charts)
    - Status distribution (reused vs success vs error)
    - Total reuse percentage with goal comparison
  - Automatic insights and recommendations
  - Export full analysis to CSV

- **Enhanced Configuration**
  - Added Environment ID field for GraphQL queries
  - Support for both REST API v2 and GraphQL Metadata API
  - Environment ID stored in session state

- **Standalone Script**
  - `api_graphql_reused.py` can now be run independently via command line
  - Environment variable configuration support
  - CSV export of reuse statistics

### Changed
- Updated README with comprehensive documentation of new tab
- Added GraphQL API documentation and usage examples
- Included new workflow examples for SLO audits
- Enhanced troubleshooting section

### Technical Details
- Integrated GraphQL pagination for large model sets
- Added `fetch_all_models_graphql()` function for efficient data fetching
- Implemented SLO calculation logic based on build_after configuration
- Added comprehensive error handling for GraphQL responses

### Features Implemented (from TODO list)
1. ✅ SLO Compliance Table with real-time execution data
2. ✅ Model Distribution by Build After Configuration (bar + pie charts)
3. ✅ Distribution of Statuses visualization (reused vs success vs error)
4. ✅ Current Total Reuse Percentage metric with goal tracking
5. ✅ Automatic insights and recommendations engine
6. ✅ Advanced filtering (status, SLO compliance, freshness config)

### API Support
- REST API v2: For freshness and run status analysis
- GraphQL Metadata API: For environment-wide reuse and SLO analysis

---

## [2.0.0] - Previous Release

- Initial Streamlit app with 4 tabs
- Summary Statistics dashboard
- Freshness Details analysis
- Run Status Details with log parsing
- Configuration management

