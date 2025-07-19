import os
import json
import time
from datetime import datetime
from jinja2 import Template

def generate_html_report(report_data, report_type):
    """Generate HTML report for migration operations"""
    # Create reports directory if it doesn't exist
    reports_dir = "reports"
    os.makedirs(reports_dir, exist_ok=True)
    
    # Get timestamp for the report filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(reports_dir, f"{report_type}_{timestamp}.html")
    
    # Load HTML template
    template_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>MongoDB Migration Report - {{ report_type }}</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                line-height: 1.6;
                margin: 0;
                padding: 20px;
                color: #333;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
            }
            h1 {
                color: #2c3e50;
                border-bottom: 2px solid #3498db;
                padding-bottom: 10px;
            }
            .summary {
                background-color: #f8f9fa;
                border-left: 4px solid #3498db;
                padding: 15px;
                margin-bottom: 20px;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
            }
            th, td {
                padding: 12px 15px;
                border: 1px solid #ddd;
                text-align: left;
            }
            th {
                background-color: #3498db;
                color: white;
            }
            tr:nth-child(even) {
                background-color: #f2f2f2;
            }
            .success {
                color: #27ae60;
            }
            .warning {
                color: #f39c12;
            }
            .error {
                color: #e74c3c;
            }
            .timestamp {
                color: #7f8c8d;
                font-size: 0.9em;
                margin-top: 5px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>MongoDB Migration Report - {{ report_type }}</h1>
            <div class="timestamp">Generated on: {{ timestamp }}</div>
            
            <div class="summary">
                <h2>Summary</h2>
                {% if report_type == 'migrate' or report_type == 'update' %}
                <p>Total Collections: {{ report_data.total_collections }}</p>
                <p>Successful: {{ report_data.success_count }}</p>
                <p>Failed: {{ report_data.failed_count }}</p>
                {% if report_data.updated_count is defined %}
                <p>Documents Updated: {{ report_data.updated_count }}</p>
                {% endif %}
                {% elif report_type == 'verify' %}
                <p>Total Collections: {{ report_data.total_collections }}</p>
                <p>Verified OK: {{ report_data.verified_ok }}</p>
                <p>Verification Failed: {{ report_data.verification_failed }}</p>
                {% endif %}
            </div>
            
            {% if report_data.collections %}
            <h2>Collection Details</h2>
            <table>
                <thead>
                    <tr>
                        <th>Collection</th>
                        {% if report_type == 'migrate' %}
                        <th>Status</th>
                        <th>Duration (s)</th>
                        {% elif report_type == 'verify' %}
                        <th>Source Count</th>
                        <th>Target Count</th>
                        <th>Match %</th>
                        <th>Status</th>
                        {% elif report_type == 'update' %}
                        <th>Documents Updated</th>
                        {% endif %}
                        <th>Details</th>
                    </tr>
                </thead>
                <tbody>
                    {% for collection in report_data.collections %}
                    <tr>
                        <td>{{ collection.name }}</td>
                        {% if report_type == 'migrate' %}
                        <td class="{{ 'success' if collection.status == 'success' else 'error' }}">
                            {{ collection.status }}
                        </td>
                        <td>{{ collection.duration|round(2) if collection.duration else 'N/A' }}</td>
                        {% elif report_type == 'verify' %}
                        <td>{{ collection.source_count }}</td>
                        <td>{{ collection.target_count }}</td>
                        <td>{{ collection.match_percentage|round(2) }}%</td>
                        <td class="{{ 'success' if collection.status == 'OK' else 'error' }}">
                            {{ collection.status }}
                        </td>
                        {% elif report_type == 'update' %}
                        <td>{{ collection.updated_count }}</td>
                        {% endif %}
                        <td>{{ collection.details if collection.details else 'N/A' }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            {% endif %}
        </div>
    </body>
    </html>
    """
    
    # Create Jinja2 template
    template = Template(template_html)
    
    # Add timestamp to report data
    report_data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Render HTML
    html_content = template.render(report_data=report_data, report_type=report_type, timestamp=report_data['timestamp'])
    
    # Write HTML to file
    with open(report_file, 'w') as f:
        f.write(html_content)
    
    return report_file

def format_migrate_report(results, update_result=None):
    """Format migration results for HTML report"""
    collections = []
    
    # Add successful collections
    for collection_name in results.get('success', []):
        collections.append({
            'name': collection_name,
            'status': 'success',
            'duration': None,
            'details': 'Migration completed successfully'
        })
    
    # Add failed collections
    for failed in results.get('failed', []):
        collections.append({
            'name': failed.get('collection', 'Unknown'),
            'status': 'failed',
            'duration': None,
            'details': failed.get('error', 'Unknown error')
        })
    
    report_data = {
        'total_collections': len(results.get('success', [])) + len(results.get('failed', [])),
        'success_count': len(results.get('success', [])),
        'failed_count': len(results.get('failed', [])),
        'collections': collections
    }
    
    if update_result is not None:
        report_data['updated_count'] = update_result
    
    return report_data

def format_verify_report(verification_results):
    """Format verification results for HTML report"""
    collections = []
    verified_ok = 0
    verification_failed = 0
    
    for result in verification_results:
        status = result.get('status', 'UNKNOWN')
        if status == 'OK':
            verified_ok += 1
        else:
            verification_failed += 1
            
        collections.append({
            'name': result.get('collection', 'Unknown'),
            'source_count': result.get('source_count', 0),
            'target_count': result.get('target_count', 0),
            'match_percentage': result.get('match_percentage', 0),
            'status': status,
            'details': 'Verification successful' if status == 'OK' else 'Count mismatch'
        })
    
    return {
        'total_collections': len(verification_results),
        'verified_ok': verified_ok,
        'verification_failed': verification_failed,
        'collections': collections
    }

def format_update_report(collections_updated):
    """Format update results for HTML report"""
    collections = []
    total_updated = 0
    
    for collection_name, updated_count in collections_updated.items():
        collections.append({
            'name': collection_name,
            'updated_count': updated_count,
            'details': f'{updated_count} documents updated'
        })
        total_updated += updated_count
    
    return {
        'total_collections': len(collections_updated),
        'success_count': len(collections_updated),
        'failed_count': 0,
        'updated_count': total_updated,
        'collections': collections
    }