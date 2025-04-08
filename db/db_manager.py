"""
Database manager for the Instalily Case Study.
Handles all database operations.
"""

import os
import sqlite3
import logging
from typing import Dict, List, Any, Optional, Tuple
import json

# Import config settings
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DB_PATH, PROCESSED_DATA_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("db_manager")

class DBManager:
    """
    Database manager for the Instalily Case Study.
    """
    
    def __init__(self, db_path: str = DB_PATH):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None
    
    def connect(self) -> None:
        """
        Connect to the database.
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Connect to the database
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            self.cursor = self.conn.cursor()
            
            logger.info(f"Connected to database: {self.db_path}")
        
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
    
    def close(self) -> None:
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def initialize_db(self) -> None:
        """
        Initialize the database schema.
        """
        try:
            if not self.conn:
                self.connect()
            
            # Read schema file
            schema_path = os.path.join(os.path.dirname(self.db_path), "schema.sql")
            
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            
            # Execute schema SQL
            self.cursor.executescript(schema_sql)
            self.conn.commit()
            
            logger.info("Database schema initialized")
        
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
    
    def import_contractors_from_json(self, json_path: str = PROCESSED_DATA_PATH) -> int:
        """
        Import contractors from a JSON file into the database.
        
        Args:
            json_path: Path to the JSON file with processed contractor data
            
        Returns:
            Number of records imported
        """
        try:
            if not self.conn:
                self.connect()
            
            # Load JSON data
            with open(json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Extract contractor data
            if isinstance(json_data, dict) and "data" in json_data:
                contractors = json_data.get("data", [])
            elif isinstance(json_data, list):
                contractors = json_data
            else:
                logger.error(f"Unexpected data format in {json_path}")
                return 0
            
            # Start a transaction
            self.conn.execute("BEGIN TRANSACTION")
            
            # Track counts
            total_imported = 0
            
            # Process each contractor
            for contractor in contractors:
                # Extract fields for contractors table
                contractor_id = contractor.get("id")
                
                if not contractor_id:
                    logger.warning("Skipping contractor without ID")
                    continue
                
                # Insert or update contractor record
                self._upsert_contractor(contractor)
                
                # Handle certifications (many-to-many)
                certifications = contractor.get("certifications", [])
                if certifications:
                    self._add_contractor_certifications(contractor_id, certifications)
                
                # Handle services (many-to-many)
                services = contractor.get("services", [])
                if services:
                    self._add_contractor_services(contractor_id, services)
                
                total_imported += 1
            
            # Commit the transaction
            self.conn.commit()
            
            logger.info(f"Imported {total_imported} contractors into database")
            return total_imported
        
        except Exception as e:
            # Rollback on error
            if self.conn:
                self.conn.rollback()
            
            logger.error(f"Error importing contractors: {str(e)}")
            raise
    
    def _upsert_contractor(self, contractor: Dict[str, Any]) -> None:
        """
        Insert or update a contractor record.
        
        Args:
            contractor: Contractor data dictionary
        """
        # Extract fields
        contractor_id = contractor.get("id")
        name = contractor.get("name")
        rating = contractor.get("rating")
        address = contractor.get("address")
        phone = contractor.get("phone")
        website = contractor.get("website")
        description = contractor.get("description")
        source = contractor.get("source")
        zip_code = contractor.get("zip_code")
        city = contractor.get("city")
        state = contractor.get("state")
        processed_date = contractor.get("processed_date")
        data_quality_score = contractor.get("data_quality_score")
        years_in_business = contractor.get("years_in_business")
        estimated_size = contractor.get("estimated_size")
        high_value_prospect = 1 if contractor.get("high_value_prospect") else 0
        
        # Check if contractor already exists
        self.cursor.execute("SELECT id FROM contractors WHERE id = ?", (contractor_id,))
        existing = self.cursor.fetchone()
        
        if existing:
            # Update existing record
            sql = """
            UPDATE contractors SET
                name = ?,
                rating = ?,
                address = ?,
                phone = ?,
                website = ?,
                description = ?,
                source = ?,
                zip_code = ?,
                city = ?,
                state = ?,
                processed_date = ?,
                data_quality_score = ?,
                years_in_business = ?,
                estimated_size = ?,
                high_value_prospect = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """
            
            self.cursor.execute(sql, (
                name, rating, address, phone, website, description, source,
                zip_code, city, state, processed_date, data_quality_score,
                years_in_business, estimated_size, high_value_prospect, contractor_id
            ))
        else:
            # Insert new record
            sql = """
            INSERT INTO contractors (
                id, name, rating, address, phone, website, description,
                source, zip_code, city, state, processed_date, data_quality_score,
                years_in_business, estimated_size, high_value_prospect
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            self.cursor.execute(sql, (
                contractor_id, name, rating, address, phone, website, description,
                source, zip_code, city, state, processed_date, data_quality_score,
                years_in_business, estimated_size, high_value_prospect
            ))
    
    def _add_contractor_certifications(self, contractor_id: str, certifications: List[str]) -> None:
        """
        Add certifications for a contractor (many-to-many relationship).
        
        Args:
            contractor_id: Contractor ID
            certifications: List of certification names
        """
        # Remove existing certifications for this contractor
        self.cursor.execute("DELETE FROM contractor_certifications WHERE contractor_id = ?", (contractor_id,))
        
        # Add each certification
        for cert in certifications:
            # Make sure the certification exists in the certifications table
            self.cursor.execute("INSERT OR IGNORE INTO certifications (name) VALUES (?)", (cert,))
            
            # Get the certification ID
            self.cursor.execute("SELECT id FROM certifications WHERE name = ?", (cert,))
            cert_id = self.cursor.fetchone()["id"]
            
            # Add the contractor-certification relationship
            self.cursor.execute(
                "INSERT OR IGNORE INTO contractor_certifications (contractor_id, certification_id) VALUES (?, ?)",
                (contractor_id, cert_id)
            )
    
    def _add_contractor_services(self, contractor_id: str, services: List[str]) -> None:
        """
        Add services for a contractor (many-to-many relationship).
        
        Args:
            contractor_id: Contractor ID
            services: List of service names
        """
        # Remove existing services for this contractor
        self.cursor.execute("DELETE FROM contractor_services WHERE contractor_id = ?", (contractor_id,))
        
        # Add each service
        for service in services:
            # Make sure the service exists in the services table
            self.cursor.execute("INSERT OR IGNORE INTO services (name) VALUES (?)", (service,))
            
            # Get the service ID
            self.cursor.execute("SELECT id FROM services WHERE name = ?", (service,))
            service_id = self.cursor.fetchone()["id"]
            
            # Add the contractor-service relationship
            self.cursor.execute(
                "INSERT OR IGNORE INTO contractor_services (contractor_id, service_id) VALUES (?, ?)",
                (contractor_id, service_id)
            )
    
    def get_contractors(self, limit: int = 100, offset: int = 0, high_value_only: bool = False) -> List[Dict[str, Any]]:
        """
        Get contractors from the database.
        
        Args:
            limit: Maximum number of contractors to return
            offset: Offset for pagination
            high_value_only: Whether to return only high-value prospects
            
        Returns:
            List of contractor dictionaries
        """
        try:
            if not self.conn:
                self.connect()
            
            # Build query
            sql = "SELECT * FROM contractors"
            
            # Add filters
            filters = []
            params = []
            
            if high_value_only:
                filters.append("high_value_prospect = 1")
            
            # Apply filters
            if filters:
                sql += " WHERE " + " AND ".join(filters)
            
            # Add sorting and pagination
            sql += " ORDER BY data_quality_score DESC, name LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            # Execute query
            self.cursor.execute(sql, params)
            
            # Convert results to list of dictionaries
            contractors = []
            for row in self.cursor.fetchall():
                # Convert Row object to dictionary
                contractor = dict(row)
                
                # Get certifications
                self.cursor.execute("""
                    SELECT c.name FROM certifications c
                    JOIN contractor_certifications cc ON c.id = cc.certification_id
                    WHERE cc.contractor_id = ?
                """, (contractor["id"],))
                
                certifications = [row["name"] for row in self.cursor.fetchall()]
                contractor["certifications"] = certifications
                
                # Get services
                self.cursor.execute("""
                    SELECT s.name FROM services s
                    JOIN contractor_services cs ON s.id = cs.service_id
                    WHERE cs.contractor_id = ?
                """, (contractor["id"],))
                
                services = [row["name"] for row in self.cursor.fetchall()]
                contractor["services"] = services
                
                contractors.append(contractor)
            
            return contractors
        
        except Exception as e:
            logger.error(f"Error getting contractors: {str(e)}")
            return []
    
    def add_insight(self, insight: Dict[str, Any]) -> Optional[int]:
        """
        Add an insight to the database.
        
        Args:
            insight: Insight data dictionary
            
        Returns:
            Insight ID if successful, None otherwise
        """
        try:
            if not self.conn:
                self.connect()
            
            # Extract fields
            contractor_id = insight.get("contractor_id")
            summary = insight.get("summary")
            engagement_strategy = insight.get("engagement_strategy")
            contact_priority = insight.get("contact_priority")
            
            # Start a transaction
            self.conn.execute("BEGIN TRANSACTION")
            
            # Insert insight
            sql = """
            INSERT INTO insights (
                contractor_id, summary, engagement_strategy, contact_priority
            ) VALUES (?, ?, ?, ?)
            """
            
            self.cursor.execute(sql, (
                contractor_id, summary, engagement_strategy, contact_priority
            ))
            
            # Get the new insight ID
            insight_id = self.cursor.lastrowid
            
            # Add selling points
            selling_points = insight.get("selling_points", [])
            for point in selling_points:
                self.cursor.execute(
                    "INSERT INTO selling_points (insight_id, point) VALUES (?, ?)",
                    (insight_id, point)
                )
            
            # Add recommended products
            recommended_products = insight.get("recommended_products", [])
            for product in recommended_products:
                self.cursor.execute(
                    "INSERT INTO recommended_products (insight_id, product) VALUES (?, ?)",
                    (insight_id, product)
                )
            
            # Commit the transaction
            self.conn.commit()
            
            return insight_id
        
        except Exception as e:
            # Rollback on error
            if self.conn:
                self.conn.rollback()
            
            logger.error(f"Error adding insight: {str(e)}")
            return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the database.
        
        Returns:
            Dictionary with database statistics
        """
        try:
            if not self.conn:
                self.connect()
            
            stats = {}
            
            # Count contractors
            self.cursor.execute("SELECT COUNT(*) as count FROM contractors")
            stats["total_contractors"] = self.cursor.fetchone()["count"]
            
            # Count high-value prospects
            self.cursor.execute("SELECT COUNT(*) as count FROM contractors WHERE high_value_prospect = 1")
            stats["high_value_prospects"] = self.cursor.fetchone()["count"]
            
            # Count by company size
            self.cursor.execute("""
                SELECT estimated_size, COUNT(*) as count
                FROM contractors
                GROUP BY estimated_size
            """)
            size_counts = {row["estimated_size"] or "Unknown": row["count"] for row in self.cursor.fetchall()}
            stats["company_sizes"] = size_counts
            
            # Average rating
            self.cursor.execute("SELECT AVG(rating) as avg_rating FROM contractors WHERE rating IS NOT NULL")
            stats["average_rating"] = self.cursor.fetchone()["avg_rating"]
            
            # Count insights
            self.cursor.execute("SELECT COUNT(*) as count FROM insights")
            stats["total_insights"] = self.cursor.fetchone()["count"]
            
            # Count by contact priority
            self.cursor.execute("""
                SELECT contact_priority, COUNT(*) as count
                FROM insights
                GROUP BY contact_priority
            """)
            priority_counts = {row["contact_priority"]: row["count"] for row in self.cursor.fetchall()}
            stats["contact_priorities"] = priority_counts
            
            return stats
        
        except Exception as e:
            logger.error(f"Error getting statistics: {str(e)}")
            return {}

def main():
    """Main function to initialize the database and import data."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Instalily Case Study Database Manager")
    parser.add_argument("--init", action="store_true", help="Initialize the database schema")
    parser.add_argument("--import", action="store_true", help="Import contractors from JSON")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--json-path", type=str, default=PROCESSED_DATA_PATH, help="Path to JSON file with contractor data")
    parser.add_argument("--db-path", type=str, default=DB_PATH, help="Path to SQLite database")
    
    args = parser.parse_args()
    
    # Create DB manager
    db_manager = DBManager(db_path=args.db_path)
    
    try:
        # Connect to database
        db_manager.connect()
        
        # Initialize database if requested
        if args.init:
            print("Initializing database schema...")
            db_manager.initialize_db()
            print(f"Database initialized at {args.db_path}")
        
        # Import data if requested
        if getattr(args, 'import'):  # Using getattr because 'import' is a Python keyword
            print(f"Importing contractors from {args.json_path}...")
            count = db_manager.import_contractors_from_json(args.json_path)
            print(f"Imported {count} contractors")
        
        # Show statistics if requested
        if args.stats:
            print("\n=== Database Statistics ===")
            stats = db_manager.get_statistics()
            
            print(f"Total contractors: {stats.get('total_contractors', 0)}")
            print(f"High-value prospects: {stats.get('high_value_prospects', 0)}")
            
            print("\nCompany Size Distribution:")
            for size, count in stats.get('company_sizes', {}).items():
                print(f"  {size}: {count}")
            
            print(f"\nAverage rating: {stats.get('average_rating', 0):.2f}")
            
            print(f"\nTotal insights: {stats.get('total_insights', 0)}")
            
            print("\nContact Priority Distribution:")
            for priority, count in sorted(stats.get('contact_priorities', {}).items()):
                print(f"  Priority {priority}: {count}")
        
        # If no actions specified, show help
        if not (args.init or getattr(args, 'import') or args.stats):
            parser.print_help()
    
    finally:
        # Close database connection
        db_manager.close()

if __name__ == "__main__":
    main()