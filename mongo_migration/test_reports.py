import os
import sys
from report_generator import (
    generate_html_report,
    format_migrate_report,
    format_verify_report,
    format_update_report,
)


def test_migrate_report():
    """Test migration report generation"""
    # Sample migration results
    results = {
        "success": ["users", "products", "orders"],
        "failed": [{"collection": "transactions", "error": "Connection timeout"}],
    }

    # Format and generate report
    report_data = format_migrate_report(results, update_result=15)
    report_file = generate_html_report(report_data, "migrate")

    print(f"Migration report generated: {report_file}")
    return report_file


def test_verify_report():
    """Test verification report generation"""
    # Sample verification results
    verification_results = [
        {
            "collection": "users",
            "source_count": 1000,
            "target_count": 1000,
            "match_percentage": 100.0,
            "status": "OK",
        },
        {
            "collection": "products",
            "source_count": 500,
            "target_count": 498,
            "match_percentage": 99.6,
            "status": "OK",
        },
        {
            "collection": "orders",
            "source_count": 750,
            "target_count": 700,
            "match_percentage": 93.3,
            "status": "MISMATCH",
        },
    ]

    # Format and generate report
    report_data = format_verify_report(verification_results)
    report_file = generate_html_report(report_data, "verify")

    print(f"Verification report generated: {report_file}")
    return report_file


def test_update_report():
    """Test update report generation"""
    # Sample update results
    collections_updated = {"users": 5, "products": 10, "orders": 20}

    # Format and generate report
    report_data = format_update_report(collections_updated)
    report_file = generate_html_report(report_data, "update")

    print(f"Update report generated: {report_file}")
    return report_file


def main():
    """Run all report tests"""
    # Create reports directory if it doesn't exist
    os.makedirs("reports", exist_ok=True)

    # Run tests
    test_migrate_report()
    test_verify_report()
    test_update_report()

    print("All reports generated successfully!")


if __name__ == "__main__":
    main()
