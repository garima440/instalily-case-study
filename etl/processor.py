"""
ETL processor for cleaning and transforming contractor data.
"""

import json
import logging
import os
from typing import Dict, List, Any, Optional
from datetime import datetime

# Import config settings
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
    CONTRACTOR_FIELDS,
    FIELD_PROCESSORS
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("etl_processor")

class ContractorDataProcessor:
    """
    Processes raw contractor data to clean, normalize, and transform it.
    """
    
    def __init__(
        self,
        input_path: str = RAW_DATA_PATH,
        output_path: str = PROCESSED_DATA_PATH
    ):
        """
        Initialize the contractor data processor.
        
        Args:
            input_path: Path to the raw contractor data
            output_path: Path to save the processed data
        """
        self.input_path = input_path
        self.output_path = output_path
    
    def load_raw_data(self) -> List[Dict[str, Any]]:
        """
        Load raw contractor data from the input file.
        
        Returns:
            List of contractor data dictionaries
        """
        try:
            if not os.path.exists(self.input_path):
                logger.error(f"Input file not found: {self.input_path}")
                return []
            
            with open(self.input_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            # Handle different data structures
            if isinstance(raw_data, dict) and "data" in raw_data:
                # New format with metadata
                return raw_data.get("data", [])
            elif isinstance(raw_data, list):
                # Old format (just a list of contractors)
                return raw_data
            else:
                logger.error(f"Unexpected data format in {self.input_path}")
                return []
                
        except Exception as e:
            logger.error(f"Error loading raw data: {str(e)}")
            return []
    
    def clean_and_normalize(self, contractors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean and normalize the contractor data.
        
        Args:
            contractors: List of raw contractor data dictionaries
            
        Returns:
            List of cleaned and normalized contractor data dictionaries
        """
        processed_contractors = []
        
        for i, contractor in enumerate(contractors):
            try:
                # Create a new dictionary with only the fields we want
                processed = {}
                
                # Process each field
                for field in CONTRACTOR_FIELDS:
                    # Get the field value (or None if not present)
                    value = contractor.get(field)
                    
                    # Apply field-specific processing if defined
                    if field in FIELD_PROCESSORS and value is not None:
                        try:
                            value = FIELD_PROCESSORS[field](value)
                        except Exception as e:
                            logger.warning(f"Error processing field '{field}' for contractor {i}: {str(e)}")
                    
                    processed[field] = value
                
                # Add derived fields
                processed["processed_date"] = datetime.now().isoformat()
                
                # Extract city and state from address
                address = processed.get("address", "")
                if address and address != "N/A":
                    try:
                        # Simple parsing - in real world would use a proper address parser
                        parts = address.split(',')
                        if len(parts) >= 2:
                            processed["city"] = parts[-2].strip() if len(parts) > 2 else ""
                            state_zip = parts[-1].strip().split()
                            processed["state"] = state_zip[0] if state_zip else ""
                    except Exception as e:
                        logger.warning(f"Error parsing address for contractor {i}: {str(e)}")
                
                # Normalize empty or missing values
                for key, value in processed.items():
                    if value in (None, "", "N/A", "Unknown"):
                        processed[key] = None
                
                # Add unique ID if not present
                if "id" not in processed:
                    # Create a simple ID based on name and address to avoid duplicates
                    name = processed.get("name", "")
                    address = processed.get("address", "")
                    if name and address:
                        import hashlib
                        id_string = f"{name}|{address}".lower()
                        processed["id"] = hashlib.md5(id_string.encode('utf-8')).hexdigest()
                    else:
                        processed["id"] = f"contractor_{i}"
                
                # Add data quality score
                processed["data_quality_score"] = self._calculate_data_quality_score(processed)
                
                processed_contractors.append(processed)
                
            except Exception as e:
                logger.error(f"Error processing contractor {i}: {str(e)}")
        
        return processed_contractors
    
    def _calculate_data_quality_score(self, contractor: Dict[str, Any]) -> float:
        """
        Calculate a data quality score for a contractor.
        
        Args:
            contractor: Contractor data dictionary
            
        Returns:
            Quality score between 0.0 and 1.0
        """
        # Define importance of each field (weights)
        field_weights = {
            "name": 1.0,
            "rating": 0.7,
            "address": 0.8,
            "phone": 0.6,
            "certifications": 0.5,
            "description": 0.4,
            "website": 0.5,
        }
        
        # Calculate weighted score
        total_weight = 0.0
        total_score = 0.0
        
        for field, weight in field_weights.items():
            total_weight += weight
            
            value = contractor.get(field)
            if value is not None:
                # For lists (like certifications), check if they're non-empty
                if isinstance(value, list):
                    score = 1.0 if value else 0.0
                # For strings, check if they're meaningful
                elif isinstance(value, str):
                    score = 1.0 if value and value != "N/A" else 0.0
                # For other types, just check if they exist
                else:
                    score = 1.0
                
                total_score += weight * score
        
        # Normalize to 0.0-1.0 range
        return total_score / total_weight if total_weight > 0 else 0.0
    
    def deduplicate(self, contractors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate contractors from the data.
        
        Args:
            contractors: List of contractor data dictionaries
            
        Returns:
            Deduplicated list of contractor data dictionaries
        """
        # Use a dictionary to track unique contractors by ID
        unique_contractors = {}
        
        for contractor in contractors:
            contractor_id = contractor.get("id")
            
            # If this ID is already in our unique contractors
            if contractor_id in unique_contractors:
                existing = unique_contractors[contractor_id]
                # Keep the one with the higher data quality score
                if contractor.get("data_quality_score", 0) > existing.get("data_quality_score", 0):
                    unique_contractors[contractor_id] = contractor
            else:
                unique_contractors[contractor_id] = contractor
        
        deduplicated = list(unique_contractors.values())
        
        logger.info(f"Deduplication: {len(contractors)} -> {len(deduplicated)} contractors")
        return deduplicated
    
    def enrich_data(self, contractors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich the contractor data with additional information.
        
        Args:
            contractors: List of contractor data dictionaries
            
        Returns:
            List of enriched contractor data dictionaries
        """
        for contractor in contractors:
            # Calculate years in business (placeholder - in real implementation
            # this would be based on real data like registration date)
            contractor["years_in_business"] = None
            
            # Estimate company size (placeholder)
            contractor["estimated_size"] = self._estimate_company_size(contractor)
            
            # Extract services offered from description
            contractor["services"] = self._extract_services(contractor.get("description", ""))
            
            # Flag high-value prospects based on certifications, rating, etc.
            contractor["high_value_prospect"] = self._is_high_value_prospect(contractor)
        
        return contractors
    
    def _estimate_company_size(self, contractor: Dict[str, Any]) -> Optional[str]:
        """
        Estimate the size of the company based on available data.
        This is a placeholder implementation that would be more sophisticated in production.
        
        Args:
            contractor: Contractor data dictionary
            
        Returns:
            Estimated company size category or None
        """
        # In a real implementation, this would use more signals like:
        # - Number of locations
        # - Years in business
        # - Number of reviews
        # - Website complexity/quality
        # - External data sources
        
        # Simplified placeholder logic
        certifications = contractor.get("certifications", [])
        cert_count = len(certifications) if certifications else 0
        
        if cert_count >= 3:
            return "Large"
        elif cert_count >= 1:
            return "Medium"
        else:
            return "Small"
    
    def _extract_services(self, description: str) -> List[str]:
        """
        Extract services offered from the contractor description.
        This is a placeholder implementation that would be more sophisticated in production.
        
        Args:
            description: Contractor description text
            
        Returns:
            List of identified services
        """
        if not description:
            return []
        
        # List of common roofing services to search for
        services = [
            "residential roofing",
            "commercial roofing",
            "roof replacement",
            "roof repair",
            "roof inspection",
            "roof maintenance",
            "emergency roof repair",
            "storm damage",
            "shingle roofing",
            "metal roofing",
            "flat roofing",
            "tile roofing",
            "slate roofing",
            "gutter installation",
            "skylight installation",
            "insulation",
            "ventilation",
        ]
        
        # Check which services are mentioned in the description
        found_services = []
        description_lower = description.lower()
        
        for service in services:
            if service in description_lower:
                found_services.append(service)
        
        return found_services
    
    def _is_high_value_prospect(self, contractor: Dict[str, Any]) -> bool:
        """
        Determine if a contractor is a high-value prospect.
        
        Args:
            contractor: Contractor data dictionary
            
        Returns:
            True if the contractor is a high-value prospect, False otherwise
        """
        # Factors that indicate a high-value prospect:
        # 1. High rating (4.5+ out of 5)
        # 2. Multiple certifications
        # 3. Complete profile (high data quality)
        # 4. Larger company size
        
        points = 0
        
        # Rating check
        rating = contractor.get("rating")
        if rating is not None and rating >= 4.5:
            points += 2
        elif rating is not None and rating >= 4.0:
            points += 1
        
        # Certifications check
        certifications = contractor.get("certifications", [])
        cert_count = len(certifications) if certifications else 0
        if cert_count >= 3:
            points += 2
        elif cert_count >= 1:
            points += 1
        
        # Data quality check
        data_quality = contractor.get("data_quality_score", 0)
        if data_quality >= 0.8:
            points += 1
        
        # Company size check
        size = contractor.get("estimated_size")
        if size == "Large":
            points += 2
        elif size == "Medium":
            points += 1
        
        # Threshold for high-value prospect
        return points >= 4
    
    def save_processed_data(self, processed_data: List[Dict[str, Any]]) -> None:
        """
        Save the processed data to the output file.
        
        Args:
            processed_data: List of processed contractor data dictionaries
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            
            # Create output structure with metadata
            output = {
                "data": processed_data,
                "metadata": {
                    "process_date": datetime.now().isoformat(),
                    "record_count": len(processed_data),
                    "source_file": self.input_path,
                }
            }
            
            with open(self.output_path, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2)
            
            logger.info(f"Saved processed data to {self.output_path}")
        
        except Exception as e:
            logger.error(f"Error saving processed data: {str(e)}")
    
    def process(self) -> List[Dict[str, Any]]:
        """
        Execute the complete ETL process.
        
        Returns:
            List of processed contractor data dictionaries
        """
        try:
            logger.info(f"Starting ETL process for {self.input_path}")
            
            # Load raw data
            raw_contractors = self.load_raw_data()
            logger.info(f"Loaded {len(raw_contractors)} raw contractor records")
            
            if not raw_contractors:
                logger.warning("No raw data found to process")
                return []
            
            # Clean and normalize
            logger.info("Cleaning and normalizing data")
            cleaned_contractors = self.clean_and_normalize(raw_contractors)
            
            # Deduplicate
            logger.info("Deduplicating records")
            unique_contractors = self.deduplicate(cleaned_contractors)
            
            # Enrich
            logger.info("Enriching data with additional information")
            enriched_contractors = self.enrich_data(unique_contractors)
            
            # Save
            self.save_processed_data(enriched_contractors)
            
            logger.info(f"ETL process complete: {len(enriched_contractors)} processed records")
            return enriched_contractors
            
        except Exception as e:
            logger.error(f"Error during ETL process: {str(e)}")
            return []

def main():
    """Main function to run the ETL processor."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Contractor Data ETL Processor")
    parser.add_argument("--input", type=str, default=RAW_DATA_PATH, help=f"Input data file path (default: {RAW_DATA_PATH})")
    parser.add_argument("--output", type=str, default=PROCESSED_DATA_PATH, help=f"Output data file path (default: {PROCESSED_DATA_PATH})")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Configure logging level
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)
    
    # Run the processor
    processor = ContractorDataProcessor(input_path=args.input, output_path=args.output)
    processed_data = processor.process()
    
    print(f"Processed {len(processed_data)} contractor records")
    print(f"Output saved to {args.output}")

if __name__ == "__main__":
    main()