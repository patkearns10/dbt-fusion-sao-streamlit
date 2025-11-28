# 🔍 dbt Freshness & Run Status Analyzer

A Streamlit web application for analyzing dbt freshness configuration and model execution patterns across your dbt Cloud projects.

Try here:
https://dbt-fusion-sao.streamlit.app/


## ✨ What's New in v2.9.0

### 🎯 Enhanced SAO Adoption Analysis

Complete **State-Aware Orchestration (SAO) analysis suite** in Environment Overview:

**SAO Metrics & Visualizations:**
- Overall adoption rate with dual charts (donut + bar)
- Scheduled jobs-specific analysis (where SAO matters most!)
- **Job Type Breakdown**: Compare SAO adoption across CI, Merge, Scheduled, and Other jobs

**Freshness Configuration Coverage:**
- Identify jobs with SAO but no freshness configs (won't reuse effectively!)
- Find jobs with freshness but no SAO (missing optimization opportunity)
- 4 key configuration patterns with color-coded insights

**Top Opportunities Analysis:**
- Prioritized list of which jobs to enable SAO on next
- Impact score based on run frequency and duration
- ROI calculator showing estimated time savings
- Color-coded by priority (High/Medium/Low)

### 📋 Model Details Enhancements

**Project/Package Management:**
- Filter models by project/package (multiselect dropdown)
- New "Project/Package" column showing model origins
- **Group by Project/Package**: Optional expandable view with per-project metrics
- Easily distinguish main project from external dependencies

### 💰 Cost Analysis Improvements

- Removed redundant "Cost Distribution by Status" chart
- Streamlined focus on actionable cost metrics

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

This will install:
- streamlit
- pandas
- requests
- numpy
- plotly

### 2. Run the App

```bash
cd /path/to/dbt-fusion-sao-streamlit
PYTHONDONTWRITEBYTECODE=1 python3 -m streamlit run streamlit_freshness_app.py
```

**Important**: Use `python3 -m streamlit` (not just `streamlit`) for proper import handling.

The app will open automatically in your browser at `http://localhost:8501`

### 3. Configure Your Credentials

1. Click on the **"⚙️ Configuration"** tab
2. Enter your dbt Cloud credentials:
   - **dbt Cloud URL**: e.g., `https://cloud.getdbt.com`
   - **API Key**: Your dbt Cloud API token (supports both REST and GraphQL)
   - **Account ID**: Your dbt Cloud account ID
   - **Job ID**: Default job ID to analyze
   - **Project ID** (optional): For filtering
   - **Environment ID** (optional): For GraphQL queries (Model Reuse & SLO Analysis)
3. Click **"💾 Save Configuration"**

You're ready to go! These credentials will be used across all tabs.

## 📊 Features

The app provides six main tabs:

### Tab 1: ⚙️ Configuration (Start Here!)

One-time setup for your dbt Cloud credentials and default job settings.
- Saves credentials in session (not persisted to disk)
- Easy to reconfigure for different jobs or environments
- No need to re-enter credentials in other tabs
- **Includes helpful descriptions of all analysis tabs**

**Perfect for**: First-time setup, switching between jobs/environments

### Tab 2: 🎯 Environment Overview (Main Dashboard)

**Your primary source of truth** for environment-wide health and performance:
- Real-time status from dbt Cloud GraphQL API
- **Key Metrics**: Reuse rate, freshness coverage, SLO compliance
- **SLO Compliance Table**: All models with execution status and configuration
- **Interactive Visualizations**: Build after distribution, status breakdown
- **SAO Adoption Analysis** (NEW!):
  - Overall and scheduled job SAO adoption metrics
  - Job type breakdown (CI, Merge, Scheduled, Other)
  - Freshness configuration coverage analysis
  - Top opportunities for enabling SAO with impact scores
  - All jobs list with SAO status
- **Automatic Insights**: AI-powered recommendations for optimization
- Filter by status, SLO compliance, and freshness configuration

**Perfect for**: Daily monitoring, stakeholder reports, SAO optimization planning

**Note**: Requires Environment ID to be configured

### Tab 3: 📋 Model Details

**Deep dive into individual model configurations** from job manifest (with project filtering):
- **Flexible Source Selection**:
  - Environment (Latest): Get latest run from environment automatically
  - Specific Job ID: Analyze a particular job's latest run
  - Specific Run ID: Direct run analysis
- **NEW: Project/Package Management**:
  - Filter by project/package (multiselect dropdown)
  - "Project/Package" column shows model origins
  - Optional "Group by Project/Package" view with per-project metrics
  - Easily distinguish main project from external dependencies
- **Run Status Filtering** - Choose Success, Error, or Cancelled runs
- **Job Type Filtering** - Filter to ci, merge, scheduled, or other jobs
- Model-by-model freshness configuration
- `warn_after`, `error_after`, `build_after` settings
- Source and model identification
- Export detailed reports

**Perfect for**: Configuration audits, cross-project analysis, troubleshooting specific models, compliance documentation

**Note**: Uses job manifest data (point-in-time snapshot)

### Tab 4: 📈 Historical Trends

**Analyze performance patterns** across multiple job runs over time:
- **⚡ Parallel processing**: Analyzes up to 10 runs simultaneously for 5-10x faster results
- **NEW: Run Status Filtering** - Analyze Success, Error, or Cancelled runs independently
- **Environment-Wide or Job-Specific**: Choose all jobs in environment or filter to one
- **Job Type Filtering** - Filter to ci, merge, scheduled, or other jobs
- **SAO Jobs Only** - Focus on State-Aware Orchestration jobs for accurate reuse metrics
- **Reuse Rate Trending**: Track reuse percentage over time with trend line
- **Goal Visualization**: 30% reuse goal line on charts
- **Trend Statistics**: Average, peak, and lowest reuse rates  
- **Trend Direction**: Calculate if reuse is improving or declining
- Success vs. reused model trends
- Interactive timeline charts
- Per-run breakdown with statistics
- Date range filtering (recommended: 14-30 days)

**Perfect for**: Weekly/monthly reviews, trend analysis, before/after comparisons

**Note**: Best with 20-50 runs for meaningful trends

### Tab 5: 💰 Cost Analysis

**Quantify the financial impact** of your dbt optimization efforts:
- **Run Status Filtering** - Analyze costs for Success, Error, or Cancelled runs
- **Environment-Wide or Job-Specific**: Choose all jobs in environment or filter to one
- **Job Type Filtering** - Filter to ci, merge, scheduled, or other jobs
- **SAO Jobs Only** - Focus on SAO jobs for accurate cost/reuse metrics
- **Cost Estimation**: Calculate costs based on execution time and warehouse type
- **Savings from Reuse**: Show money saved from cache hits using actual execution times
- **ROI Analysis**: Measure return on investment from freshness configurations
- **Cost Trends**: Visualize cost patterns over time
- **Top Expensive Models**: Identify the 20 most costly models
- **Cost Breakdown**: Per-run and model-level cost analysis
- **Multi-Currency Support**: USD, EUR, GBP, CAD, AUD
- **Warehouse Configuration**: Support for 8 warehouse sizes (X-Small to 4X-Large)
- _(Removed redundant "Cost Distribution by Status" chart - focused on actionable metrics)_

**Perfect for**: Stakeholder reporting, optimization prioritization, budget planning

**Note**: Supports Snowflake pricing with customizable cost per hour

### Tab 6: 🔀 Job Overlap Analysis (NEW!)

**Identify models being run by multiple jobs** (pre-SAO optimization):
- **Job Inventory**: List all jobs in the environment
- **Overlap Detection**: Find models executed in multiple jobs
- **Overlap Ranking**: Bar chart of most duplicated models
- **Waste Calculation**: Quantify redundant executions
- **Job-to-Job Matrix**: Visual overlap between jobs
- **Priority Recommendations**: Based on overlap severity
- **Export Options**: Download mappings and reports

**Perfect for**: Pre-SAO environments, job consolidation, reducing waste, audit efficiency

**Note**: In environments without State-Aware Orchestration (SAO), multiple jobs may unnecessarily run the same models. This analysis helps identify and eliminate that waste.

## 🎯 Key Metrics Explained

### Freshness Configuration Coverage (Goal: 80%)

**What it measures**: Percentage of models and sources with freshness configurations defined.

**Why it matters**: Freshness configs help detect stale data and trigger model rebuilds when needed.

**Interpretation**:
- ✅ **≥80%**: Excellent coverage
- ⚠️ **<80%**: Opportunity to add more freshness configs

### Model Reuse Rate (Goal: 30%)

**What it measures**: Percentage of models that were reused from cache (not re-executed).

**Why it matters**: Higher reuse rate = less compute time = lower costs = faster runs.

**Interpretation**:
- ✅ **≥30%**: Good incremental strategy
- 🎯 **50-90%**: Excellent optimization
- ⚠️ **<30%**: Consider adding more incremental models

## 💡 Usage Tips

### For Daily Monitoring:
```
1. Open "Summary Statistics"
2. Click "Generate Summary"
3. Check if metrics meet goals
4. Done! (< 30 seconds)
```

### For Deep Analysis:
```
1. Check "Summary Statistics" first
2. Drill into "Freshness Details" for specific models
3. Use "Run Status Details" for trends over 7-30 days
4. Export data for further analysis
```

### For Performance Optimization:
```
1. Run "Run Status Details" for 14-30 days
2. Look for models that always run (never reused)
3. Investigate in "Freshness Details"
4. Add incremental configs or build_after rules
```

## 🔧 Advanced Usage

### Analyzing Multiple Jobs

1. Click "🔄 Reconfigure" in sidebar
2. Change Job ID
3. Click "Save Configuration"
4. All tabs now use the new job

### Performance Tips for Run Analysis

- **Parallel processing enabled**: Up to 10 runs analyzed simultaneously
- **Recommended**: 20-50 runs for comprehensive analysis (only takes 10-20 seconds)
- **Date range**: Use 14-30 days for trending analysis
- Each run requires 2-4 API calls (run metadata, manifest, run_results.json per step) - done in parallel
- 20 runs typically takes ~10-15 seconds (vs 60-100 seconds sequential)

### Understanding Run Statuses

The app uses a **step-based approach** to get accurate statuses from `run_results.json`:

1. **Filters to relevant steps**: Only analyzes `dbt run` and `dbt build` commands
2. **Fetches run_results.json**: Gets actual execution results from each step  
3. **Aggregates results**: Combines data across steps for complete accuracy

**Status values:**
- **success**: Model executed successfully
- **skipped**: Model was skipped (often due to deferral/reuse in SAO jobs)
- **error**: Model execution failed

This approach eliminates all guesswork and heuristics - we use dbt's actual execution status!

## 🐛 Troubleshooting

### NumPy Import Error

If you see:
```
ImportError: Error importing numpy...
```

**Solution**: Make sure to use `python3 -m streamlit`:
```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m streamlit run streamlit_freshness_app.py
```

### "No runs found for job"

- Verify Job ID is correct
- Check Project ID (try leaving it blank)
- Ensure the job has at least one completed run

### API Authentication Errors

- Verify API key is valid and has proper permissions
- Check Account ID is correct
- Ensure API key has access to the specified job

### Run Status Analysis Performance

- **New parallel processing**: Analyzes up to 10 runs simultaneously
- **Expected speed**: ~5-10 seconds for 20 runs (vs 40-100 seconds sequential)
- **Recommended**: Feel free to analyze 20-50 runs at once
- **Progress indicator**: Watch the progress bar to track parallel analysis

## 📂 Files

- **`streamlit_freshness_app.py`**: Main Streamlit application with all analysis tabs
- **`log_freshness.py`**: Core logic for fetching and processing dbt artifacts
- **`log_freshness_from_job.py`**: Helper for fetching job runs
- **`api_graphql_reused.py`**: Standalone script for GraphQL-based reuse analysis (also integrated into Streamlit app)
- **`requirements.txt`**: Python dependencies

## 🔗 dbt Cloud API

This app uses both dbt Cloud REST API v2 and GraphQL API:
- **REST API v2**: [API Documentation](https://docs.getdbt.com/dbt-cloud/api-v2)
  - Used for freshness analysis and run status tracking
  - Fetches manifest.json, run_results.json, and run logs
- **GraphQL API**: [Metadata API](https://docs.getdbt.com/docs/dbt-cloud-apis/metadata-api)
  - Used for environment-wide model reuse and SLO analysis
  - Provides real-time execution information and configuration data
- Requires an API key with appropriate read permissions
- Supports both user tokens and service account tokens

## 📝 Notes

- API keys are stored in session memory only (not persisted)
- Results are computed fresh each time (not cached)
- For large projects, analysis may take 30-90 seconds
- Internet connection required for dbt Cloud API access

## 🆘 Support

For issues or questions:
1. Check the [dbt Cloud API documentation](https://docs.getdbt.com/dbt-cloud/api-v2)
2. Verify your API credentials and permissions
3. Review console output for detailed error messages

## 🎯 Common Workflows

### Morning Health Check (< 30 seconds)
1. Open app (credentials already saved)
2. Summary Statistics → Generate Summary
3. Quick glance at goal comparisons
4. Done!

### Weekly Optimization Review (5-10 minutes)
1. Summary Statistics → See overall health
2. Freshness Details → Identify models without configs
3. Run Status Details → Analyze 30-50 runs over 14-30 days (only takes ~15 seconds!)
4. Look for optimization opportunities
5. Export data for action items

### Stakeholder Demo (2-3 minutes)
1. Summary Statistics → Show goals and metrics
2. Freshness Details → Filter to their team's models
3. Run Status Details → Chart of last 30 days
4. Export CSV for their review

### Environment SLO Audit (3-5 minutes)
1. Model Reuse & SLO Analysis → Enter Environment ID
2. Review key metrics: Total reuse rate, SLO compliance
3. Check Build After Distribution charts
4. Review models outside of SLO
5. Follow insights & recommendations
6. Export full analysis to CSV

## 🚀 Best Practices

1. **Configure once per session**: Save time by setting up credentials in the Configuration tab
2. **Start with Summary**: Get the big picture before drilling into details
3. **Use date filters**: Narrow your analysis scope for faster results
4. **Export data**: Download CSV/JSON for sharing or further analysis
5. **Track trends**: Run weekly to monitor improvements over time

## 🔧 Standalone Script Usage

The `api_graphql_reused.py` script can also be run independently for command-line analysis:

```bash
# Set environment variables
export DBT_CLOUD_API_KEY="your_api_key_here"
export DBT_CLOUD_ENVIRONMENT_ID="672"

# Run the script
python3 api_graphql_reused.py
```

This will fetch all models, calculate reuse statistics, and save results to CSV. However, the Streamlit app provides a much richer interactive experience with visualizations and filtering capabilities.

---

**Version**: 2.0  
**Updated**: November 2025  
**Author**: dbt Labs Field Engineering



