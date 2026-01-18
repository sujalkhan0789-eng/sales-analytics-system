"""
API Handler Module
Handles external API integration for fetching product information
"""

import requests
from typing import Dict, Optional, List
import time


class APIHandler:
    """Handles API calls for product information"""
    
    # Mock API endpoint (in real scenario, this would be your company's API)
    MOCK_API_URL = "https://fakestoreapi.com/products"
    
    @staticmethod
    def fetch_product_info(product_id: str) -> Optional[Dict]:
        """
        Fetch product information from API
        
        Args:
            product_id: Product ID to fetch info for
            
        Returns:
            Dictionary with product info or None if not found
        """
        try:
            # For demo purposes, using a mock API
            # In a real scenario, you would use your company's product API
            response = requests.get(f"{APIHandler.MOCK_API_URL}/{product_id[-2:]}", 
                                   timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'api_product_id': str(data.get('id', '')),
                    'title': data.get('title', 'Unknown'),
                    'price': data.get('price', 0),
                    'category': data.get('category', 'Unknown'),
                    'description': data.get('description', '')[:100] + '...',
                    'rating': data.get('rating', {}).get('rate', 0)
                }
            else:
                print(f"API Error for {product_id}: {response.status_code}")
                return None
                
        except requests.RequestException as e:
            print(f"Network error fetching product {product_id}: {str(e)}")
            return None
        except Exception as e:
            print(f"Error fetching product {product_id}: {str(e)}")
            return None
    
    @staticmethod
    def enrich_products_data(valid_records: List[Dict]) -> List[Dict]:
        """
        Enrich sales data with product information from API
        
        Args:
            valid_records: List of valid sales records
            
        Returns:
            List of enriched sales records
        """
        enriched_records = []
        unique_product_ids = set(r['ProductID'] for r in valid_records)
        
        print(f"\nFetching product information for {len(unique_product_ids)} unique products...")
        
        product_info_cache = {}
        
        # Fetch product information for each unique product
        for product_id in unique_product_ids:
            info = APIHandler.fetch_product_info(product_id)
            if info:
                product_info_cache[product_id] = info
            # Add delay to avoid hitting rate limits
            time.sleep(0.1)
        
        # Enrich each record with product information
        for record in valid_records:
            enriched_record = record.copy()
            product_id = record['ProductID']
            
            if product_id in product_info_cache:
                enriched_record['ProductInfo'] = product_info_cache[product_id]
            else:
                enriched_record['ProductInfo'] = {
                    'title': record['ProductName'],
                    'price': 'N/A',
                    'category': 'Unknown',
                    'description': 'No additional information available',
                    'rating': 'N/A'
                }
            
            enriched_records.append(enriched_record)
        
        print(f"Successfully enriched {len(enriched_records)} records with product information")
        return enriched_records
    
    @staticmethod
    def get_product_categories(enriched_records: List[Dict]) -> Dict:
        """
        Extract product categories from enriched data
        
        Args:
            enriched_records: List of enriched sales records
            
        Returns:
            Dictionary with category analysis
        """
        categories = {}
        
        for record in enriched_records:
            product_info = record.get('ProductInfo', {})
            category = product_info.get('category', 'Unknown')
            
            if category not in categories:
                categories[category] = {
                    'total_sales': 0,
                    'total_quantity': 0,
                    'products': set()
                }
            
            categories[category]['total_sales'] += record.get('TotalPrice', 0)
            categories[category]['total_quantity'] += record.get('Quantity', 0)
            categories[category]['products'].add(record['ProductID'])
        
        # Convert sets to lists for JSON serialization
        for category in categories:
            categories[category]['products'] = list(categories[category]['products'])
            categories[category]['unique_products'] = len(categories[category]['products'])
        
        return categories