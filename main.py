"""
Main Module - Sales Data Analytics System
Entry point for the application
"""

import sys
import os
from datetime import datetime
from utils.file_handler import FileHandler
from utils.data_processor import DataProcessor
from utils.api_handler import APIHandler


def main():
    """Main function to run the sales analytics system"""
    
    print("=" * 60)
    print("SALES DATA ANALYTICS SYSTEM")
    print("=" * 60)
    
    # File paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_dir, "data")
    output_dir = os.path.join(current_dir, "output")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    input_file = os.path.join(data_dir, "sales_data.txt")
    clean_data_file = os.path.join(output_dir, "cleaned_sales_data.csv")
    summary_file = os.path.join(output_dir, "cleaning_summary.txt")
    report_file = os.path.join(output_dir, "sales_report.json")
    analysis_report_file = os.path.join(output_dir, "analysis_report.txt")
    
    # Step 1: Read the sales data file
    print("\nStep 1: Reading sales data file...")
    lines, total_records = FileHandler.read_sales_file(input_file)
    
    if total_records == 0:
        print("No records found. Exiting...")
        sys.exit(1)
    
    print(f"Found {total_records} records in the file")
    
    # Step 2: Parse and clean the data
    print("\nStep 2: Parsing and cleaning data...")
    
    # Parse all lines (skip header)
    raw_records = []
    for line in lines[1:]:  # Skip header
        parsed_record = FileHandler.parse_line(line)
        if parsed_record:
            raw_records.append(parsed_record)
    
    print(f"Successfully parsed {len(raw_records)} records")
    
    # Clean and validate records
    valid_records, invalid_records = DataProcessor.clean_and_validate_records(raw_records)
    
    # Display cleaning results
    print("\n" + "=" * 60)
    print("DATA CLEANING RESULTS:")
    print("=" * 60)
    print(f"Total records parsed: {len(raw_records)}")
    print(f"Invalid records removed: {len(invalid_records)}")
    print(f"Valid records after cleaning: {len(valid_records)}")
    
    # Save cleaning summary
    FileHandler.save_summary(
        len(valid_records),
        len(invalid_records),
        len(raw_records),
        summary_file
    )
    
    # Display some invalid records for debugging
    if invalid_records:
        print("\nSample of invalid records removed:")
        print("-" * 40)
        for i, record in enumerate(invalid_records[:3]):  # Show first 3
            print(f"{i+1}. TransactionID: {record.get('TransactionID')}")
            print(f"   Error: {record.get('Error', 'Unknown error')}")
            print(f"   Product: {record.get('ProductName')}")
            print()
    
    if len(valid_records) == 0:
        print("No valid records found after cleaning. Exiting...")
        sys.exit(1)
    
    # Step 3: Save cleaned data
    print("\nStep 3: Saving cleaned data...")
    FileHandler.save_clean_data(valid_records, clean_data_file)
    
    # Step 4: Perform sales analysis
    print("\nStep 4: Performing sales analysis...")
    analysis = DataProcessor.analyze_sales(valid_records)
    
    # Generate and display sales report
    sales_report = DataProcessor.generate_sales_report(analysis)
    print("\n" + sales_report)
    
    # Save analysis report as text
    with open(analysis_report_file, 'w') as f:
        f.write(sales_report)
    
    # Step 5: Enrich data with API information (optional)
    print("\nStep 5: Enriching data with external product information...")
    try:
        enriched_records = APIHandler.enrich_products_data(valid_records)
        
        # Analyze product categories from API data
        category_analysis = APIHandler.get_product_categories(enriched_records)
        
        # Add category analysis to main analysis
        analysis['product_categories'] = category_analysis
        
        print(f"\nProduct Categories Found:")
        print("-" * 40)
        for category, data in category_analysis.items():
            print(f"{category}:")
            print(f"  Sales: ${data['total_sales']:,.2f}")
            print(f"  Quantity: {data['total_quantity']:,}")
            print(f"  Unique Products: {data['unique_products']}")
        
    except Exception as e:
        print(f"Warning: Could not fetch API data: {str(e)}")
        print("Continuing with local analysis only...")
    
    # Step 6: Save comprehensive report
    print("\nStep 6: Saving comprehensive report...")
    
    # Add timestamp and system info to report
    final_report = {
        'generated_at': datetime.now().isoformat(),
        'input_file': input_file,
        'cleaning_summary': {
            'total_parsed': len(raw_records),
            'invalid_removed': len(invalid_records),
            'valid_kept': len(valid_records)
        },
        'analysis': analysis,
        'sample_valid_records': valid_records[:5]  # Include first 5 valid records as sample
    }
    
    FileHandler.save_report(final_report, report_file)
    
    # Final output
    print("\n" + "=" * 60)
    print("PROCESS COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"\nOutput Files Generated:")
    print(f"1. Cleaned Data: {clean_data_file}")
    print(f"2. Cleaning Summary: {summary_file}")
    print(f"3. Analysis Report: {analysis_report_file}")
    print(f"4. Comprehensive Report: {report_file}")
    
    # Display key findings
    print(f"\nKey Findings:")
    print(f"- Total Sales: ${analysis['summary'].get('total_sales', 0):,.2f}")
    print(f"- Top Region: {max(analysis['by_region'].items(), key=lambda x: x[1]['total_sales'])[0]}")
    
    if 'product_categories' in analysis:
        top_category = max(analysis['product_categories'].items(), 
                          key=lambda x: x[1]['total_sales'])[0]
        print(f"- Top Product Category: {top_category}")
    
    print("\nSystem ready for business decision making!")


if __name__ == "__main__":
    main()