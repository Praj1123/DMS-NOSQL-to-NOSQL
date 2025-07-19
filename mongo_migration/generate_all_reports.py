import os
import json
import sys
from datetime import datetime
from report_generator import generate_html_report, format_migrate_report, format_verify_report, format_update_report

def load_progress_data():
    """Load progress data from progress directory"""
    progress_data = {}
    progress_dir = "progress"
    
    for filename in os.listdir(progress_dir):
        if filename.endswith(".json") and not filename.endswith("_cdc.json"):
            collection_name = filename.split(".")[0]
            with open(os.path.join(progress_dir, filename), "r") as f:
                data = json.load(f)
                progress_data[collection_name] = data
    
    return progress_data

def load_verification_data():
    """Load the most recent verification data"""
    verification_dir = "verification"
    verification_files = [f for f in os.listdir(verification_dir) if f.startswith("verification_") and f.endswith(".json")]
    
    if not verification_files:
        return []
    
    # Get the most recent verification file
    latest_file = sorted(verification_files)[-1]
    with open(os.path.join(verification_dir, latest_file), "r") as f:
        return json.load(f)

def generate_all_reports():
    """Generate all reports in one go"""
    # Create reports directory if it doesn't exist
    os.makedirs("reports", exist_ok=True)
    
    # Load progress data
    progress_data = load_progress_data()
    
    # Create migration report
    migrate_results = {"success": [], "failed": []}
    total_docs = 0
    
    for collection, data in progress_data.items():
        migrate_results["success"].append(collection)
        total_docs += data.get("count", 0)
    
    # Generate migration report
    migrate_report_data = format_migrate_report(migrate_results, total_docs)
    migrate_report_file = generate_html_report(migrate_report_data, "migrate")
    print(f"Migration report generated: {migrate_report_file}")
    
    # Load verification data
    verification_results = load_verification_data()
    
    # Generate verification report
    if verification_results:
        verify_report_data = format_verify_report(verification_results)
        verify_report_file = generate_html_report(verify_report_data, "verify")
        print(f"Verification report generated: {verify_report_file}")
    
    # Generate update report (using progress data as a proxy)
    collections_updated = {collection: data.get("count", 0) for collection, data in progress_data.items()}
    update_report_data = format_update_report(collections_updated)
    update_report_file = generate_html_report(update_report_data, "update")
    print(f"Update report generated: {update_report_file}")
    
    # Generate dashboard report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dashboard_file = os.path.join("reports", f"dashboard_{timestamp}.html")
    
    with open(dashboard_file, "w") as f:
        f.write(f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MongoDB Migration Dashboard</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 2px solid #3498db;
        }}
        .dashboard {{
            display: flex;
            flex-direction: column;
            gap: 20px;
        }}
        .report-frame {{
            width: 100%;
            height: 600px;
            border: 1px solid #ddd;
        }}
        .tabs {{
            display: flex;
            gap: 10px;
            margin-bottom: 10px;
        }}
        .tab {{
            padding: 10px 20px;
            background-color: #f8f9fa;
            border: 1px solid #ddd;
            cursor: pointer;
        }}
        .tab.active {{
            background-color: #3498db;
            color: white;
        }}
        .report-container {{
            display: none;
        }}
        .report-container.active {{
            display: block;
        }}
    </style>
</head>
<body>
    <h1>MongoDB Migration Dashboard</h1>
    <p>Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    
    <div class="tabs">
        <div class="tab active" onclick="showReport('migrate')">Migration Report</div>
        <div class="tab" onclick="showReport('verify')">Verification Report</div>
        <div class="tab" onclick="showReport('update')">Update Report</div>
    </div>
    
    <div class="dashboard">
        <div class="report-container active" id="migrate-container">
            <iframe class="report-frame" src="{os.path.basename(migrate_report_file)}"></iframe>
        </div>
        <div class="report-container" id="verify-container">
            <iframe class="report-frame" src="{os.path.basename(verify_report_file) if verification_results else ''}"></iframe>
        </div>
        <div class="report-container" id="update-container">
            <iframe class="report-frame" src="{os.path.basename(update_report_file)}"></iframe>
        </div>
    </div>
    
    <script>
        function showReport(reportType) {{
            // Hide all report containers
            document.querySelectorAll('.report-container').forEach(container => {{
                container.classList.remove('active');
            }});
            
            // Show selected report container
            document.getElementById(reportType + '-container').classList.add('active');
            
            // Update tab styling
            document.querySelectorAll('.tab').forEach(tab => {{
                tab.classList.remove('active');
            }});
            
            // Find the clicked tab and make it active
            event.target.classList.add('active');
        }}
    </script>
</body>
</html>""")
    
    print(f"Dashboard report generated: {dashboard_file}")
    return dashboard_file

if __name__ == "__main__":
    dashboard_file = generate_all_reports()
    
    # Try to open the dashboard in the default browser
    try:
        import webbrowser
        webbrowser.open(f"file://{os.path.abspath(dashboard_file)}")
    except Exception as e:
        print(f"Could not open dashboard in browser: {e}")