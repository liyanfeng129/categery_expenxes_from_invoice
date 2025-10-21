from difflib import SequenceMatcher
import json
from collections import defaultdict
import os
import re
from s3_client import S3_Client

class CompanyCache:
    def __init__(self,  company_name: str, s3:S3_Client):
        """Initialize category cache with optional file persistence"""
        self.company_name = company_name
        self.s3 = s3
        # company_name -> {description: category}
        self.cache = defaultdict(dict)
        self.load_cache()
    
    def load_cache(self):
        """Load existing cache from s3 if it exists, create new one if not"""
    
        
        
    def save_cache(self):
        """Save cache to file"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(dict(self.cache), f, ensure_ascii=False, indent=2)
            print(f"Cache saved with {len(self.cache)} companies")
        except Exception as e:
            print(f"Error saving cache: {e}")
    
    def get_category(self, company, description):
        """Get cached category for company-description pair"""
        if company in self.cache and description in self.cache[company]:
            return self.cache[company][description]
        return None
    
    def set_category(self, company, description, category):
        """Cache category for company-description pair"""
        self.cache[company][description] = category
    
    def has_category(self, company, description):
        """Check if category is cached for company-description pair or similar description exists"""
        # Check if company exists in cache first
        if company not in self.cache:
            return False
        
        # Check for exact match first
        if description in self.cache[company]:
            return True
        
        # Check for similar descriptions (80% similarity)
        if self.cache[company]:  # company exists and has descriptions
            cached_descriptions = self.cache[company].keys()
            similar_desc = self._find_similar_description(description, cached_descriptions)
            return similar_desc is not None
        
        return False
        
    def _find_similar_description(self, description, description_set, threshold=0.8):
        """Find a similar description in the given set of descriptions"""
        for cached_desc in description_set:
            similarity = SequenceMatcher(None, self._clean_text(description), self._clean_text(cached_desc)).ratio()
            if similarity >= threshold:
                return cached_desc
        return None
    
    def _clean_text(self, text):
        text = text.lower()
        text = re.sub(r'[^a-z\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    