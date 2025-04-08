"""
AI-powered insights generator for contractor data.
Generates actionable sales intelligence using OpenAI's GPT models.
"""

import json
import logging
import os
import time
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
import tiktoken
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from openai import OpenAI

# Import config settings
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import (
    PROCESSED_DATA_PATH,
    INSIGHTS_DATA_PATH,
    OPENAI_API_KEY,
    OPENAI_MODEL,
    OPENAI_TEMPERATURE,
    OPENAI_MAX_TOKENS
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("insights_generator")

# Check if OpenAI API key is set
if not OPENAI_API_KEY:
    logger.error("OPENAI_API_KEY is not set. Please set it in your environment variables or .env file.")
    raise ValueError("OPENAI_API_KEY is not set")

class ContractorInsightsGenerator:
    """
    Generate AI-powered insights from contractor data using OpenAI.
    """
    
    def __init__(
        self,
        input_path: str = PROCESSED_DATA_PATH,
        output_path: str = INSIGHTS_DATA_PATH,
        model: str = OPENAI_MODEL,
        temperature: float = OPENAI_TEMPERATURE,
        max_tokens: int = OPENAI_MAX_TOKENS
    ):
        """
        Initialize the contractor insights generator.
        
        Args:
            input_path: Path to the processed contractor data
            output_path: Path to save the generated insights
            model: OpenAI model to use
            temperature: Temperature parameter for generation
            max_tokens: Maximum tokens for generation
        """
        self.input_path = input_path
        self.output_path = output_path
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        
        # Initialize token counter for the current encoding
        self.encoding = tiktoken.encoding_for_model(model)
    
    def load_processed_data(self) -> List[Dict[str, Any]]:
        """
        Load processed contractor data from the input file.
        
        Returns:
            List of processed contractor data dictionaries
        """
        try:
            if not os.path.exists(self.input_path):
                logger.error(f"Input file not found: {self.input_path}")
                return []
            
            with open(self.input_path, 'r', encoding='utf-8') as f:
                processed_data = json.load(f)
            
            # Handle different data structures
            if isinstance(processed_data, dict) and "data" in processed_data:
                # Format with metadata
                return processed_data.get("data", [])
            elif isinstance(processed_data, list):
                # Just a list of contractors
                return processed_data
            else:
                logger.error(f"Unexpected data format in {self.input_path}")
                return []
                
        except Exception as e:
            logger.error(f"Error loading processed data: {str(e)}")
            return []
    
    @retry(
        retry=retry_if_exception_type((Exception)),  
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30)
    )
    async def generate_insight(self, contractor: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate insights for a single contractor using OpenAI.
        
        Args:
            contractor: Processed contractor data dictionary
            
        Returns:
            Dictionary with generated insights
        """
        try:
            # Extract relevant information for prompt
            name = contractor.get("name", "Unknown")
            rating = contractor.get("rating")
            rating_str = f"{rating:.1f}" if rating is not None else "Unknown"
            description = contractor.get("description", "")
            certifications = contractor.get("certifications", [])
            cert_str = ", ".join(certifications) if certifications else "None"
            years_in_business = contractor.get("years_in_business", "Unknown")
            estimated_size = contractor.get("estimated_size", "Unknown")
            services = contractor.get("services", [])
            services_str = ", ".join(services) if services else "Unknown"
            
            # Construct the prompt with explicit instructions for JSON formatting
            prompt = f"""
            You are an AI assistant for a roofing distributor's sales team. Generate actionable sales intelligence for the following contractor:
            
            Name: {name}
            Rating: {rating_str}
            Description: {description}
            Certifications: {cert_str}
            Years in Business: {years_in_business}
            Estimated Size: {estimated_size}
            Services: {services_str}
            
            Based on the information above, provide the following insights:
            1. A concise summary of the contractor, highlighting their strengths and potential areas of focus based on available data.
            2. Key selling points for approaching this contractor.
            3. Recommended products or services to prioritize based on their profile.
            4. Suggested engagement strategy for a sales representative.
            
            The response MUST be formatted as a valid JSON object with the following fields, and nothing else:
            - summary: A paragraph summarizing the contractor
            - selling_points: An array of 2-4 strings for key selling points
            - recommended_products: An array of 2-4 strings for recommended products or services
            - engagement_strategy: A paragraph suggesting how to approach this contractor
            - contact_priority: A number from 1-5 on how high a priority this contractor should be (5 being highest)
            
            Example format:
            {{
              "summary": "Contractor description here",
              "selling_points": ["Point 1", "Point 2", "Point 3"],
              "recommended_products": ["Product 1", "Product 2", "Product 3"],
              "engagement_strategy": "Strategy description here",
              "contact_priority": 4
            }}
            
            Make the insights specific, actionable, and based only on the information provided. The goal is to help sales representatives have more effective conversations with contractors.
            """
            
            # Count tokens in the prompt
            prompt_tokens = len(self.encoding.encode(prompt))
            logger.debug(f"Prompt for '{name}' uses {prompt_tokens} tokens")
            
            # Initialize the client
            client = OpenAI(api_key=OPENAI_API_KEY)
            
            # Call OpenAI API with the current client library
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an AI assistant for a roofing distributor's sales team. You MUST respond with valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                response_format={"type": "json_object"},  # Explicitly request JSON
                n=1
            )
            
            # Extract the generated text
            generated_text = response.choices[0].message.content.strip()
            
            # Save the raw response for debugging
            os.makedirs("logs", exist_ok=True)
            with open(f"logs/openai_response_{name.replace(' ', '_')}.txt", "w", encoding="utf-8") as f:
                f.write(generated_text)
                
            logger.debug(f"Raw response for {name}: {generated_text}")
            
            # Parse the JSON response
            try:
                import json
                insight_data = json.loads(generated_text)
                
                # Add contractor ID
                insight_data["contractor_id"] = contractor.get("id")
                
                # Add generation metadata
                insight_data["generated_at"] = datetime.now().isoformat()
                insight_data["model"] = self.model
                
                logger.info(f"Generated insight for contractor: {name}")
                return insight_data
                
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing insight for {name}: {str(e)}")
                logger.debug(f"Generated text: {generated_text}")
                
                # Try to manually extract insights from unstructured text
                # This is a fallback if JSON parsing fails
                summary = ""
                selling_points = []
                recommended_products = []
                engagement_strategy = ""
                contact_priority = 3  # Default to medium priority
                
                lines = generated_text.split("\n")
                current_section = None
                
                for line in lines:
                    line = line.strip()
                    
                    if "summary" in line.lower() and ":" in line:
                        current_section = "summary"
                        summary = line.split(":", 1)[1].strip()
                    elif "selling point" in line.lower() and ":" in line:
                        current_section = "selling_points"
                    elif "recommended product" in line.lower() and ":" in line:
                        current_section = "recommended_products"
                    elif "engagement strategy" in line.lower() and ":" in line:
                        current_section = "engagement_strategy"
                        engagement_strategy = line.split(":", 1)[1].strip()
                    elif "contact priority" in line.lower() and ":" in line:
                        try:
                            # Extract number from line
                            priority_text = line.split(":", 1)[1].strip()
                            # Find any digit in the text
                            import re
                            priority_match = re.search(r'\d', priority_text)
                            if priority_match:
                                contact_priority = int(priority_match.group(0))
                        except:
                            pass
                    elif current_section == "selling_points" and line.startswith("-"):
                        selling_points.append(line[1:].strip())
                    elif current_section == "recommended_products" and line.startswith("-"):
                        recommended_products.append(line[1:].strip())
                
                # Construct a manual insight object
                manual_insight = {
                    "contractor_id": contractor.get("id"),
                    "summary": summary or generated_text[:500],
                    "selling_points": selling_points or ["Quality roofing products", "Technical support"],
                    "recommended_products": recommended_products or ["Premium shingles", "Roofing accessories"],
                    "engagement_strategy": engagement_strategy or "Approach with focus on quality and support",
                    "contact_priority": contact_priority,
                    "generated_at": datetime.now().isoformat(),
                    "model": self.model,
                    "manually_parsed": True
                }
                
                logger.info(f"Created manually parsed insight for {name}")
                return manual_insight
        
        except Exception as e:
            logger.error(f"Error generating insight for {contractor.get('name', 'Unknown')}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    async def generate_insights(self, batch_size: int = 5) -> List[Dict[str, Any]]:
        """
        Generate insights for all contractors.
        
        Args:
            batch_size: Number of contractors to process in parallel
            
        Returns:
            List of generated insights
        """
        # Load processed contractor data
        contractors = self.load_processed_data()
        
        if not contractors:
            logger.error("No contractor data found to generate insights for")
            return []
        
        logger.info(f"Generating insights for {len(contractors)} contractors")
        
        # Process contractors in batches to avoid hitting rate limits
        all_insights = []
        
        # Process in batches
        for i in range(0, len(contractors), batch_size):
            batch = contractors[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(contractors) + batch_size - 1)//batch_size}")
            
            # Generate insights for the batch in parallel
            batch_tasks = [self.generate_insight(contractor) for contractor in batch]
            batch_insights = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Filter out exceptions
            valid_insights = []
            for j, result in enumerate(batch_insights):
                if isinstance(result, Exception):
                    logger.error(f"Error generating insight for {batch[j].get('name', 'Unknown')}: {str(result)}")
                else:
                    valid_insights.append(result)
            
            all_insights.extend(valid_insights)
            
            # Save incremental progress
            self.save_insights(all_insights)
            
            # Respect rate limits with a short delay between batches
            await asyncio.sleep(2)
        
        logger.info(f"Generated {len(all_insights)} insights")
        return all_insights
    
    def save_insights(self, insights: List[Dict[str, Any]]) -> None:
        """
        Save generated insights to a file.
        
        Args:
            insights: List of insight dictionaries
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            
            # Create output structure with metadata
            output = {
                "data": insights,
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "model": self.model,
                    "count": len(insights)
                }
            }
            
            # Save to file
            with open(self.output_path, 'w', encoding='utf-8') as f:
                json.dump(output, f, indent=2)
            
            logger.info(f"Saved {len(insights)} insights to {self.output_path}")
        
        except Exception as e:
            logger.error(f"Error saving insights: {str(e)}")
    
    def load_insights(self) -> List[Dict[str, Any]]:
        """
        Load previously generated insights from file.
        
        Returns:
            List of insight dictionaries
        """
        try:
            if not os.path.exists(self.output_path):
                logger.warning(f"No insights file found at {self.output_path}")
                return []
            
            with open(self.output_path, 'r', encoding='utf-8') as f:
                insights_data = json.load(f)
            
            # Extract insights
            if isinstance(insights_data, dict) and "data" in insights_data:
                insights = insights_data.get("data", [])
            elif isinstance(insights_data, list):
                insights = insights_data
            else:
                logger.error(f"Unexpected data format in {self.output_path}")
                return []
            
            logger.info(f"Loaded {len(insights)} insights from {self.output_path}")
            return insights
        
        except Exception as e:
            logger.error(f"Error loading insights: {str(e)}")
            return []
    
    def import_insights_to_db(self, db_path: Optional[str] = None) -> int:
        """
        Import insights into the database.
        
        Args:
            db_path: Optional database path
            
        Returns:
            Number of insights imported
        """
        try:
            # Import the DBManager here to avoid circular imports
            from db.db_manager import DBManager
            
            # Load insights
            insights = self.load_insights()
            
            if not insights:
                logger.warning("No insights to import into database")
                return 0
            
            # Create DB manager
            db_manager = DBManager(db_path=db_path)
            
            try:
                # Connect to database
                db_manager.connect()
                
                # Import each insight
                count = 0
                for insight in insights:
                    insight_id = db_manager.add_insight(insight)
                    if insight_id:
                        count += 1
                
                logger.info(f"Imported {count} insights into database")
                return count
            
            finally:
                # Close database connection
                db_manager.close()
        
        except Exception as e:
            logger.error(f"Error importing insights to database: {str(e)}")
            return 0

async def main():
    """Main function to run the insights generator."""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Contractor Insights Generator")
    parser.add_argument("--input", type=str, default=PROCESSED_DATA_PATH, help="Input data file path")
    parser.add_argument("--output", type=str, default=INSIGHTS_DATA_PATH, help="Output insights file path")
    parser.add_argument("--model", type=str, default=OPENAI_MODEL, help="OpenAI model to use")
    parser.add_argument("--batch-size", type=int, default=5, help="Batch size for parallel processing")
    parser.add_argument("--import-db", action="store_true", help="Import insights into database")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Configure logging level
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)
    
    # Check if OpenAI API key is set
    if not OPENAI_API_KEY:
        print("Error: OPENAI_API_KEY environment variable not set")
        print("Please set it with: export OPENAI_API_KEY=your_api_key")
        return
    
    # Create the insights generator
    generator = ContractorInsightsGenerator(
        input_path=args.input,
        output_path=args.output,
        model=args.model
    )
    
    # Generate insights
    print(f"Generating insights using {args.model}...")
    insights = await generator.generate_insights(batch_size=args.batch_size)
    
    print(f"Generated {len(insights)} insights")
    print(f"Insights saved to {args.output}")
    
    # Import to database if requested
    if args.import_db:
        print("Importing insights into database...")
        count = generator.import_insights_to_db()
        print(f"Imported {count} insights into database")

if __name__ == "__main__":
    asyncio.run(main())