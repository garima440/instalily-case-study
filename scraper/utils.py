"""
Utility functions for the GAF scraper.
"""
import os
import time
import random
import logging
import functools
import asyncio
from typing import Callable, TypeVar, Any, Dict, Optional, List

# Configure logging
logger = logging.getLogger("gaf_scraper.utils")

# Rate limiter for controlling request frequency
class RateLimiter:
    """Controls the rate of requests to avoid overloading the target website."""
    
    def __init__(self, requests_per_minute: int = 20):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_minute: Maximum number of requests per minute
        """
        self.delay = 60.0 / requests_per_minute
        self.last_request_time = 0.0
        self.lock = asyncio.Lock()
    
    async def wait(self):
        """Wait if necessary to comply with the rate limit."""
        async with self.lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            
            if time_since_last_request < self.delay:
                wait_time = self.delay - time_since_last_request
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
            
            self.last_request_time = time.time()

# Type variable for decorators
T = TypeVar('T')

def create_directory_if_not_exists(directory_path: str) -> None:
    """
    Create a directory if it doesn't exist.
    
    Args:
        directory_path: Path to the directory
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logger.info(f"Created directory: {directory_path}")

def setup_retry_mechanism(max_retries: int = 3, delay: int = 2):
    """
    Decorator to retry a function if it raises an exception.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Base delay between retries in seconds
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            retries = 0
            while retries <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries: {str(e)}")
                        raise
                    
                    # Exponential backoff with jitter
                    wait_time = delay * (2 ** (retries - 1)) + random.uniform(0, 1)
                    logger.warning(f"Retrying {func.__name__} in {wait_time:.2f}s after error: {str(e)}")
                    time.sleep(wait_time)
        
        return wrapper
    
    return decorator

def generate_random_user_agent() -> str:
    """
    Generate a random user agent string.
    
    Returns:
        Random user agent string
    """
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36",
    ]
    return random.choice(user_agents)

def get_common_request_headers() -> dict:
    """
    Get common request headers to mimic a browser.
    
    Returns:
        Dictionary of HTTP headers
    """
    return {
        "User-Agent": generate_random_user_agent(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

def calculate_zip_code_distance(zip1: str, zip2: str) -> float:
    """
    Calculate an approximate distance between two zip codes.
    Note: This is a placeholder function. For a real implementation, you would
    want to use a proper geolocation API or library.
    
    Args:
        zip1: First ZIP code
        zip2: Second ZIP code
        
    Returns:
        Approximate distance in miles
    """
    # This is just a placeholder - in a real implementation, you would
    # use a proper ZIP code distance calculation
    return 0.0  # Return placeholder value

class ProxyManager:
    """Manages a pool of proxy servers for rotating IP addresses."""
    
    def __init__(self, proxies: Optional[List[str]] = None):
        """
        Initialize proxy manager.
        
        Args:
            proxies: List of proxy URLs in format "http://user:pass@host:port"
        """
        self.proxies = proxies or []
        self.current_index = 0
        self.lock = asyncio.Lock()
        
        if not self.proxies:
            logger.warning("No proxies provided. Requests will use direct connection.")
    
    async def get_next_proxy(self) -> Optional[str]:
        """
        Get the next proxy from the pool in a round-robin fashion.
        
        Returns:
            Proxy URL or None if no proxies available
        """
        if not self.proxies:
            return None
            
        async with self.lock:
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            return proxy
    
    def mark_proxy_bad(self, proxy: str) -> None:
        """
        Mark a proxy as bad (e.g., if it's blocked or not working).
        In a production system, you might want to remove it temporarily
        or have more sophisticated retry logic.
        
        Args:
            proxy: Proxy URL to mark as bad
        """
        if proxy in self.proxies:
            logger.warning(f"Marking proxy as bad: {proxy}")
            # For a simple implementation, we just log it
            # In a production system, you might want to remove it temporarily

def validate_contractor_data(data: Dict[str, Any]) -> bool:
    """
    Validate contractor data to ensure it has required fields.
    
    Args:
        data: Contractor data dictionary
        
    Returns:
        True if data is valid, False otherwise
    """
    required_fields = ["name", "address"]
    
    # Check if all required fields are present and not empty
    for field in required_fields:
        if field not in data or not data[field]:
            logger.warning(f"Validation failed: Missing required field '{field}'")
            return False
    
    # Additional validation can be added here as needed
    return True

async def handle_captcha(page) -> bool:
    """
    Detect and handle CAPTCHA challenges.
    In a production environment, you might want to use a CAPTCHA solving service.
    
    Args:
        page: Playwright page object
        
    Returns:
        True if CAPTCHA was handled successfully, False otherwise
    """
    # Check for common CAPTCHA indicators
    captcha_selectors = [
        "iframe[src*='captcha']",
        "iframe[src*='recaptcha']",
        ".g-recaptcha",
        "form[action*='captcha']",
        "input[name*='captcha']"
    ]
    
    for selector in captcha_selectors:
        if await page.locator(selector).count() > 0:
            logger.warning("CAPTCHA detected on page")
            
            # In a production system, you would integrate with a CAPTCHA solving service here
            # For this example, we'll just simulate failure
            return False
    
    return True  # No CAPTCHA detected