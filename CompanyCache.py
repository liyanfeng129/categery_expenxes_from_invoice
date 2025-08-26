from difflib import SequenceMatcher
import json
from collections import defaultdict
import os
import re

class CompanyCache:
    def __init__(self, cache_file='company_cache.json'):
        """Initialize category cache with optional file persistence"""
        self.cache_file = cache_file
        # company_name -> {description: category}
        self.cache = defaultdict(dict)
        self.load_cache()
    
    def load_cache(self):
        """Load existing cache from file if it exists, create new one if not"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.cache = defaultdict(dict, data)
                print(f"Loaded cache with {len(self.cache)} companies")
            except Exception as e:
                print(f"Error loading cache: {e}")
        else:
            # Create new empty cache if file doesn't exist
            self.cache = defaultdict(dict)
            print(f"Cache file '{self.cache_file}' not found. Created new empty cache.")
        
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
        return self.cache[company].get(description)
    
    def set_category(self, company, description, category):
        """Cache category for company-description pair"""
        self.cache[company][description] = category
    
    def has_category(self, company, description):
        """Check if category is cached for company-description pair or similar description exists"""
        # Check for exact match first
        if description in self.cache[company]:
            return True
        
       # Check for similar descriptions (80% similarity)
        if company in self.cache and self.cache[company]: #company exist and has no empty descriptions
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
    def _clean_text(text):
        # Lowercase
        text = text.lower()
        # Remove numbers and special chars, keep letters and spaces
        text = re.sub(r'[^a-z\s]', ' ', text)
        # Collapse multiple spaces
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    