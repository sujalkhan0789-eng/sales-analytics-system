"""
File Handler Module
Handles reading, writing, and encoding-related operations for sales data
"""

import pandas as pd
from typing import List, Dict, Optional, Tuple
import json


class FileHandler:
    """Handles file operations for sales data"""
    
    @staticmethod
    def read_sales_file(file_path: str) -> Tuple[List[str], int]:
        """
        Read sales data file with proper encoding handling
        
        Args:
            file_path: Path to the sales data file
            
        Returns:
            Tuple of (list of lines, total records read)
        """
        lines = []
        total_records = 0
        
        try:
            # Try different encodings to handle non-UTF-8 files
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as file:
                        lines = file.readlines()
                        total_records = len(lines) - 1  # Exclude header
                        print(f"Successfully read file with {encoding} encoding")
                        break
                except UnicodeDecodeError:
                    continue
                    
            if not lines:
                raise ValueError("Could not read file with any supported encoding")
                
        except FileNotFoundError:
            print(f"Error: File not found at {file_path}")
            return [], 0
        except Exception as e:
            print(f"Error reading file: {str(e)}")
            return [], 0
            
        return lines, total_records
    
    @staticmethod
    def parse_line(line: str, delimiter: str = '|') -> Optional[Dict]:
        """
        Parse a single line of sales data
        
        Args:
            line: Line from sales data file
            delimiter: Field delimiter
            
        Returns:
            Dictionary of parsed data or None if invalid
        """
        # Skip empty lines
        if not line.strip():
            return None
            
        # Remove newline and split by delimiter
        fields = line.strip().split(delimiter)
        
        # Skip lines with incorrect number of fields
        if len(fields) != 8:
            return None
            
        return {
            'TransactionID': fields[0].strip(),
            'Date': fields[1].strip(),
            'ProductID': fields[2].strip(),
            'ProductName': fields[3].strip(),
            'Quantity': fields[4].strip(),
            'UnitPrice': fields[5].strip(),
            'CustomerID': fields[6].strip(),
            'Region': fields[7].strip()
        }
    
    @staticmethod
    def save_clean_data(data: List[Dict], output_path: str) -> bool:
        """
        Save cleaned data to a CSV file
        
        Args:
            data: List of cleaned data dictionaries
            output_path: Path to save the cleaned data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            df = pd.DataFrame(data)
            df.to_csv(output_path, index=False)
            print(f"Cleaned data saved to {output_path}")
            return True
        except Exception as e:
            print(f"Error saving data: {str(e)}")
            return False
    
    @staticmethod
    def save_report(report_data: Dict, report_path: str) -> bool:
        """
        Save analysis report to a JSON file
        
        Args:
            report_data: Dictionary containing report data
            report_path: Path to save the report
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(report_path, 'w') as file:
                json.dump(report_data, file, indent=4)
            print(f"Report saved to {report_path}")
            return True
        except Exception as e:
            print(f"Error saving report: {str(e)}")
            return False
    
    @staticmethod
    def save_summary(valid_count: int, invalid_count: int, total_records: int, 
                     output_path: str) -> bool:
        """
        Save cleaning summary to a text file
        
        Args:
            valid_count: Number of valid records
            invalid_count: Number of invalid records
            total_records: Total records parsed
            output_path: Path to save the summary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with open(output_path, 'w') as file:
                file.write("=== DATA CLEANING SUMMARY ===\n")
                file.write(f"Total records parsed: {total_records}\n")
                file.write(f"Invalid records removed: {invalid_count}\n")
                file.write(f"Valid records after cleaning: {valid_count}\n")
            print(f"Summary saved to {output_path}")
            return True
        except Exception as e:
            print(f"Error saving summary: {str(e)}")
            return False