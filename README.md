# 🔍 dbt Freshness & Run Status Analyzer

A Streamlit web application for analyzing dbt freshness configuration and model execution patterns across your dbt Cloud projects.

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
   - **API Key**: Your dbt Cloud API token
   - **Account ID**: Your dbt Cloud account ID
   - **Job ID**: Default job ID to analyze
   - **Project ID** (optional): For filtering
3. Click **"💾 Save Configuration"**

You're ready to go! These credentials will be used across all tabs.

## 📊 Features

The app provides four main tabs:

### Tab 1: 📊 Summary Statistics (Main Dashboard)

Quick health check showing:
- **Freshness Configuration Coverage**: % of models/sources with freshness configs (Goal: 80%)
- **Model Reuse Rate**: % of models reused from cache (Goal: 30%)
- Breakdown by resource type
- Model execution status overview

**Perfect for**: Daily check-ins, stakeholder demos

### Tab 2: ⚙️ Configuration

One-time setup for your dbt Cloud credentials and default job settings.
- Saves credentials in session (not persisted to disk)
- Easy to reconfigure for different jobs
- No need to re-enter credentials in other tabs

### Tab 3: 📋 Freshness Details

Detailed analysis of freshness configuration:
- Model-by-model freshness breakdown
- `warn_after`, `error_after`, `build_after` configurations
- Filter by resource type
- Export to CSV/JSON

**Perfect for**: Audits, investigating specific models, compliance

### Tab 4: 📈 Run Status Details

Analyze model execution patterns over time:
- Success vs. reused model trends
- Interactive stacked bar charts
- Per-run breakdown with statistics
- Date range filtering (default: last 7 days)
- Accurate "reused" status from log parsing

**Perfect for**: Weekly reviews, optimization planning, tracking efficiency

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

- **Start small**: Begin with 5-10 runs, then increase
- **Narrow date range**: Shorter periods = faster analysis
- Each run requires 3 API calls (run metadata, manifest, logs)
- 10 runs typically takes 30-45 seconds

### Understanding Run Statuses

The app parses dbt Cloud logs to extract accurate statuses:
- **success**: Model executed successfully
- **reused**: Model was reused from cache (incremental/deferred)
- **error**: Model execution failed

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

### Run Status Analysis is Slow

- **Expected**: 2-5 seconds per run analyzed
- **Reduce runs**: Use slider to analyze fewer runs (5-10)
- **Shorten date range**: Try last 7 days instead of 30
- **Progress indicator**: Watch the progress bar to track analysis

## 📂 Files

- **`streamlit_freshness_app.py`**: Main Streamlit application
- **`log_freshness.py`**: Core logic for fetching and processing dbt artifacts
- **`log_freshness_from_job.py`**: Helper for fetching job runs
- **`requirements.txt`**: Python dependencies

## 🔗 dbt Cloud API

This app uses the dbt Cloud API v2:
- [API Documentation](https://docs.getdbt.com/dbt-cloud/api-v2)
- Requires an API key with job read permissions
- Fetches manifest.json, run_results.json, and run logs

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
3. Run Status Details → Analyze 20 runs over 14 days
4. Look for optimization opportunities
5. Export data for action items

### Stakeholder Demo (2-3 minutes)
1. Summary Statistics → Show goals and metrics
2. Freshness Details → Filter to their team's models
3. Run Status Details → Chart of last 30 days
4. Export CSV for their review

## 🚀 Best Practices

1. **Configure once per session**: Save time by setting up credentials in the Configuration tab
2. **Start with Summary**: Get the big picture before drilling into details
3. **Use date filters**: Narrow your analysis scope for faster results
4. **Export data**: Download CSV/JSON for sharing or further analysis
5. **Track trends**: Run weekly to monitor improvements over time

---

**Version**: 2.0  
**Updated**: November 2025  
**Author**: dbt Labs Field Engineering

