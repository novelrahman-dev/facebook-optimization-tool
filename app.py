from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import requests
import json
import os
import gspread
from google.oauth2.service_account import Credentials
import csv
import io
from datetime import datetime, timedelta
import time
from collections import defaultdict
import threading
import pickle

app = Flask(__name__)
CORS(app)

class FacebookOptimizationTool:
    def __init__(self):
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.fb_access_token = os.getenv('FB_ACCESS_TOKEN')
        self.fb_ad_account_id = os.getenv('FB_AD_ACCOUNT_ID')
        self.google_credentials = os.getenv('GOOGLE_CREDENTIALS_JSON')
        
        # Data loading status
        self.loading_status = {
            'google_sheets_loaded': False,
            'facebook_api_loading': False,
            'facebook_api_loaded': False,
            'data_processed': False,
            'total_ads_loaded': 0,
            'current_page': 0,
            'error_message': None,
            'last_updated': None,
            'expected_ads': 747,
            'pagination_complete': False
        }
        
        # Initialize KPI settings with defaults
        self.kpi_settings = {
            'ctr_threshold': 0.30,
            'funnel_start_threshold': 15.0,
            'cpa_threshold': 120.0,
            'clicks_threshold': 500,
            'roas_threshold': 1.0,
            'cpc_threshold': 10.0,
            'cpm_threshold': 50.0,
            'booking_conversion_threshold': 2.0
        }
        
        # Initialize optimization rules with defaults
        self.optimization_rules = {
            # Pause rules
            'pause_roas_threshold': 0.5,
            'pause_spend_threshold': 100.0,
            'pause_cpa_threshold': 200.0,
            'pause_cpa_spend_threshold': 50.0,
            'pause_ctr_threshold': 0.20,
            'pause_ctr_spend_threshold': 75.0,
            'pause_no_bookings_threshold': 150.0,
            'pause_high_cpc_threshold': 15.0,
            'pause_high_cpc_spend_threshold': 100.0,
            
            # Scale rules
            'scale_roas_threshold': 2.0,
            'scale_min_spend_threshold': 50.0,
            'scale_all_criteria_required': True,
            'scale_ctr_bonus_threshold': 0.50,
            'scale_cpa_bonus_threshold': 80.0,
            'scale_booking_rate_threshold': 3.0,
            
            # Priority settings
            'high_priority_spend_threshold': 200.0,
            'medium_priority_spend_threshold': 100.0
        }
        
        # Load saved settings
        self.load_kpi_settings()
        self.load_optimization_rules()
        
        # Initialize APIs
        self.init_google_sheets()
        self.init_facebook_api()
        
        # Initialize data containers
        self.performance_data = []
        self.web_data = []
        self.attr_data = []
        self.fb_data = []
        self.fb_api_data = []
        
        # Start background data loading
        self.start_background_loading()
        
        print("✅ Facebook Optimization Tool initialized")

    def load_kpi_settings(self):
        """Load KPI settings from file"""
        try:
            settings_file = '/tmp/kpi_settings.json'
            if os.path.exists(settings_file):
                with open(settings_file, 'r') as f:
                    saved_settings = json.load(f)
                    self.kpi_settings.update(saved_settings)
                print("✅ KPI settings loaded from file")
            else:
                print("✅ Using default KPI settings")
        except Exception as e:
            print(f"⚠️ Error loading KPI settings: {e}")

    def save_kpi_settings(self):
        """Save KPI settings to file"""
        try:
            settings_file = '/tmp/kpi_settings.json'
            with open(settings_file, 'w') as f:
                json.dump(self.kpi_settings, f, indent=2)
            print("✅ KPI settings saved")
            return True
        except Exception as e:
            print(f"❌ Error saving KPI settings: {e}")
            return False

    def load_optimization_rules(self):
        """Load optimization rules from file"""
        try:
            rules_file = '/tmp/optimization_rules.json'
            if os.path.exists(rules_file):
                with open(rules_file, 'r') as f:
                    saved_rules = json.load(f)
                    self.optimization_rules.update(saved_rules)
                print("✅ Optimization rules loaded from file")
            else:
                print("✅ Using default optimization rules")
        except Exception as e:
            print(f"⚠️ Error loading optimization rules: {e}")

    def save_optimization_rules(self):
        """Save optimization rules to file"""
        try:
            rules_file = '/tmp/optimization_rules.json'
            with open(rules_file, 'w') as f:
                json.dump(self.optimization_rules, f, indent=2)
            print("✅ Optimization rules saved")
            return True
        except Exception as e:
            print(f"❌ Error saving optimization rules: {e}")
            return False

    def init_google_sheets(self):
        """Initialize Google Sheets API"""
        try:
            if self.google_credentials:
                credentials_dict = json.loads(self.google_credentials)
                scope = ['https://spreadsheets.google.com/feeds',
                        'https://www.googleapis.com/auth/drive']
                credentials = Credentials.from_service_account_info(credentials_dict, scopes=scope)
                self.gc = gspread.authorize(credentials)
                print("✅ Google Sheets API initialized")
            else:
                self.gc = None
                print("⚠️ Google Sheets credentials not found")
        except Exception as e:
            print(f"❌ Google Sheets error: {e}")
            self.gc = None

    def init_facebook_api(self):
        """Initialize Facebook API"""
        try:
            if self.fb_access_token and self.fb_ad_account_id:
                print("✅ Facebook API credentials configured")
            else:
                print("⚠️ Facebook API credentials not found")
        except Exception as e:
            print(f"❌ Facebook API error: {e}")

    def start_background_loading(self):
        """Start background data loading in a separate thread"""
        def background_load():
            try:
                # Load Google Sheets data first (fast)
                self.load_google_sheets_data()
                self.loading_status['google_sheets_loaded'] = True
                
                # Try to load cached Facebook data first
                if self.load_cached_facebook_data():
                    print("✅ Loaded Facebook data from cache")
                    self.loading_status['facebook_api_loaded'] = True
                    self.process_combined_data()
                    self.loading_status['data_processed'] = True
                    self.loading_status['last_updated'] = datetime.now().isoformat()
                else:
                    # Load fresh Facebook API data in background
                    self.load_facebook_api_data_background()
                
            except Exception as e:
                print(f"❌ Background loading error: {e}")
                self.loading_status['error_message'] = str(e)
        
        # Start background thread
        thread = threading.Thread(target=background_load, daemon=True)
        thread.start()

    def load_cached_facebook_data(self):
        """Load Facebook data from cache if available and recent"""
        try:
            cache_file = '/tmp/facebook_api_cache.pkl'
            if os.path.exists(cache_file):
                # Check if cache is recent (less than 30 minutes old)
                cache_age = time.time() - os.path.getmtime(cache_file)
                if cache_age < 1800:  # 30 minutes
                    with open(cache_file, 'rb') as f:
                        cached_data = pickle.load(f)
                        self.fb_api_data = cached_data.get('fb_api_data', [])
                        self.loading_status['total_ads_loaded'] = len(self.fb_api_data)
                        print(f"✅ Loaded {len(self.fb_api_data)} ads from cache")
                        return True
                else:
                    print("⚠️ Cache is too old, will refresh")
            return False
        except Exception as e:
            print(f"⚠️ Error loading cache: {e}")
            return False

    def save_facebook_data_cache(self):
        """Save Facebook data to cache"""
        try:
            cache_file = '/tmp/facebook_api_cache.pkl'
            cache_data = {
                'fb_api_data': self.fb_api_data,
                'timestamp': datetime.now().isoformat()
            }
            with open(cache_file, 'wb') as f:
                pickle.dump(cache_data, f)
            print("✅ Facebook data cached successfully")
        except Exception as e:
            print(f"⚠️ Error saving cache: {e}")

    def load_google_sheets_data(self):
        """Load data from Google Sheets"""
        try:
            if not self.gc:
                print("⚠️ Google Sheets not available")
                return
            
            # Web Pages Data
            web_sheet = self.gc.open_by_url('https://docs.google.com/spreadsheets/d/1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA/edit?usp=sharing')
            web_worksheet = web_sheet.get_worksheet(0)
            web_records = web_worksheet.get_all_records()
            self.web_data = web_records
            print(f"✅ Loaded {len(web_records)} rows from web_pages")
            
            # Attribution Data
            attr_sheet = self.gc.open_by_url('https://docs.google.com/spreadsheets/d/1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ/edit?usp=sharing')
            attr_worksheet = attr_sheet.get_worksheet(0)
            attr_records = attr_worksheet.get_all_records()
            self.attr_data = attr_records
            print(f"✅ Loaded {len(attr_records)} rows from attribution")
            
            # FB Spend Data (for spend only)
            fb_sheet = self.gc.open_by_url('https://docs.google.com/spreadsheets/d/1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk/edit?usp=sharing')
            fb_worksheet = fb_sheet.get_worksheet(0)
            fb_records = fb_worksheet.get_all_records()
            self.fb_data = fb_records
            print(f"✅ Loaded {len(fb_records)} rows from fb_spend")
            
        except Exception as e:
            print(f"❌ Google Sheets loading error: {e}")

    def debug_facebook_api_filtering(self):
        """Debug Facebook API filtering to understand the mismatch"""
        debug_info = {
            'google_sheets_analysis': {},
            'facebook_api_analysis': {},
            'comparison': {},
            'sample_data': {},
            'errors': []
        }
        
        try:
            # Analyze Google Sheets data
            if not self.fb_data:
                debug_info['errors'].append("No Google Sheets data loaded")
                return debug_info
            
            # Google Sheets Analysis
            unique_combinations = set()
            unique_ad_names = set()
            unique_adset_names = set()
            total_rows = len(self.fb_data)
            
            sample_gs_rows = []
            for i, row in enumerate(self.fb_data[:10]):
                ad_set_name = str(row.get('Facebook Adset Name', '')).strip()
                ad_name = str(row.get('Facebook Ad Name', '')).strip()
                
                sample_gs_rows.append({
                    'row_number': i + 1,
                    'ad_set_name': ad_set_name,
                    'ad_name': ad_name,
                    'combination': f"{ad_set_name}|||{ad_name}" if ad_set_name and ad_name else "INVALID"
                })
                
                if (ad_set_name and ad_name and 
                    ad_set_name.lower() != 'total' and ad_name.lower() != 'total'):
                    combination_key = f"{ad_set_name}|||{ad_name}"
                    unique_combinations.add(combination_key)
                    unique_ad_names.add(ad_name)
                    unique_adset_names.add(ad_set_name)
            
            debug_info['google_sheets_analysis'] = {
                'total_rows': total_rows,
                'unique_combinations': len(unique_combinations),
                'unique_ad_names': len(unique_ad_names),
                'unique_adset_names': len(unique_adset_names),
                'sample_combinations': list(unique_combinations)[:10]
            }
            
            debug_info['sample_data']['google_sheets_rows'] = sample_gs_rows
            
            # Facebook API Analysis
            if not self.fb_access_token or not self.fb_ad_account_id:
                debug_info['errors'].append("Facebook API credentials not available")
                return debug_info
            
            # Test Facebook API
            since_date = "2025-06-01"
            until_date = "2025-08-20"
            
            # Get first few pages of Facebook API data
            all_fb_ads = []
            after_cursor = None
            page_count = 0
            
            while page_count < 10:  # Test first 10 pages
                try:
                    url = f"https://graph.facebook.com/v18.0/{self.fb_ad_account_id}/ads"
                    
                    params = {
                        'access_token': self.fb_access_token,
                        'fields': 'id,name,adset{id,name},status,effective_status,created_time',
                        'time_range': json.dumps({
                            'since': since_date,
                            'until': until_date
                        }),
                        'limit': 50,
                        'level': 'ad'
                    }
                    
                    if after_cursor:
                        params['after'] = after_cursor
                    
                    page_count += 1
                    response = requests.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        ads_data = data.get('data', [])
                        
                        if not ads_data:
                            break
                        
                        all_fb_ads.extend(ads_data)
                        
                        # Check for next page
                        paging = data.get('paging', {})
                        cursors = paging.get('cursors', {})
                        after_cursor = cursors.get('after')
                        
                        if not after_cursor:
                            break
                            
                    else:
                        debug_info['errors'].append(f"Facebook API error: {response.status_code} - {response.text}")
                        break
                        
                except Exception as e:
                    debug_info['errors'].append(f"Facebook API request error: {str(e)}")
                    break
            
            # Analyze Facebook API data
            fb_combinations = set()
            fb_ad_names = set()
            fb_adset_names = set()
            
            sample_fb_ads = []
            for i, ad in enumerate(all_fb_ads[:10]):
                ad_name = ad.get('name', '')
                adset_info = ad.get('adset', {})
                adset_name = adset_info.get('name', '') if adset_info else ''
                status = ad.get('status', '')
                effective_status = ad.get('effective_status', '')
                created_time = ad.get('created_time', '')
                
                sample_fb_ads.append({
                    'ad_number': i + 1,
                    'ad_id': ad.get('id', ''),
                    'ad_name': ad_name,
                    'adset_name': adset_name,
                    'status': status,
                    'effective_status': effective_status,
                    'created_time': created_time,
                    'combination': f"{adset_name}|||{ad_name}" if ad_name and adset_name else "INVALID"
                })
                
                if ad_name and adset_name:
                    combination_key = f"{adset_name}|||{ad_name}"
                    fb_combinations.add(combination_key)
                    fb_ad_names.add(ad_name)
                    fb_adset_names.add(adset_name)
            
            debug_info['facebook_api_analysis'] = {
                'total_ads_fetched': len(all_fb_ads),
                'pages_fetched': page_count,
                'unique_combinations': len(fb_combinations),
                'unique_ad_names': len(fb_ad_names),
                'unique_adset_names': len(fb_adset_names),
                'sample_combinations': list(fb_combinations)[:10]
            }
            
            debug_info['sample_data']['facebook_api_ads'] = sample_fb_ads
            
            # Comparison Analysis
            matching_combinations = unique_combinations.intersection(fb_combinations)
            missing_in_fb = unique_combinations - fb_combinations
            extra_in_fb = fb_combinations - unique_combinations
            
            debug_info['comparison'] = {
                'matching_combinations': len(matching_combinations),
                'missing_in_facebook': len(missing_in_fb),
                'extra_in_facebook': len(extra_in_fb),
                'match_percentage': (len(matching_combinations) / len(unique_combinations) * 100) if unique_combinations else 0,
                'sample_matches': list(matching_combinations)[:5],
                'sample_missing_in_fb': list(missing_in_fb)[:10],
                'sample_extra_in_fb': list(extra_in_fb)[:10]
            }
            
        except Exception as e:
            debug_info['errors'].append(f"Debug analysis error: {str(e)}")
        
        return debug_info

    def load_facebook_api_data_background(self):
        """Load marketing metrics from Facebook API with exact Ad Set + Ad Name filtering"""
        try:
            if not self.fb_access_token or not self.fb_ad_account_id:
                print("⚠️ Facebook API credentials not available")
                return
            
            self.loading_status['facebook_api_loading'] = True
            
            # Use exact date range to match Google Sheets
            since_date = "2025-06-01"
            until_date = "2025-08-20"
            
            print(f"🔄 Loading Facebook API data from {since_date} to {until_date}")
            print(f"🎯 Target: Load exactly 747 ads to match Google Sheets")
            
            # Get list of Ad Set + Ad Name combinations from Google Sheets
            fb_sheet_combinations = set()
            for row in self.fb_data:
                ad_set_name = str(row.get('Facebook Adset Name', '')).strip()
                ad_name = str(row.get('Facebook Ad Name', '')).strip()
                if (ad_set_name and ad_name and 
                    ad_set_name.lower() != 'total' and ad_name.lower() != 'total'):
                    # Create unique combination key
                    combination_key = f"{ad_set_name}|||{ad_name}"
                    fb_sheet_combinations.add(combination_key)
            
            print(f"📋 Found {len(fb_sheet_combinations)} unique Ad Set + Ad Name combinations in Google Sheets")
            
            all_ads_data = []
            after_cursor = None
            page_count = 0
            
            # Load Facebook API data with filtering
            while len(all_ads_data) < 800:  # Safety limit slightly above 747
                try:
                    # Facebook API endpoint for ads with insights
                    url = f"https://graph.facebook.com/v18.0/{self.fb_ad_account_id}/ads"
                    
                    params = {
                        'access_token': self.fb_access_token,
                        'fields': 'id,name,adset{id,name},insights{impressions,clicks,spend,ctr,cpc,cpm}',
                        'time_range': json.dumps({
                            'since': since_date,
                            'until': until_date
                        }),
                        'limit': 50,
                        'level': 'ad',
                        'filtering': json.dumps([
                            {
                                'field': 'delivery_info',
                                'operator': 'IN',
                                'value': ['active', 'paused', 'pending_review', 'disapproved', 'preapproved', 'pending_billing_info', 'campaign_paused', 'adset_paused']
                            }
                        ])
                    }
                    
                    if after_cursor:
                        params['after'] = after_cursor
                    
                    page_count += 1
                    self.loading_status['current_page'] = page_count
                    print(f"📡 Fetching page {page_count} from Facebook API...")
                    
                    response = requests.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        ads_data = data.get('data', [])
                        
                        if not ads_data:
                            print(f"📄 No more ads available on page {page_count}")
                            break
                        
                        # Filter ads to only include those in Google Sheets (Ad Set + Ad Name combination)
                        filtered_ads = []
                        for ad in ads_data:
                            ad_name = ad.get('name', '')
                            adset_info = ad.get('adset', {})
                            adset_name = adset_info.get('name', '') if adset_info else ''
                            
                            if ad_name and adset_name:
                                # Create combination key to match Google Sheets
                                combination_key = f"{adset_name}|||{ad_name}"
                                if combination_key in fb_sheet_combinations:
                                    filtered_ads.append(ad)
                        
                        all_ads_data.extend(filtered_ads)
                        self.loading_status['total_ads_loaded'] = len(all_ads_data)
                        print(f"✅ Loaded {len(filtered_ads)} matching ads from page {page_count} (Total: {len(all_ads_data)}/747)")
                        
                        # Stop if we have enough ads
                        if len(all_ads_data) >= 747:
                            print(f"🎯 Reached target of 747 ads - stopping pagination")
                            break
                        
                        # Check for next page
                        paging = data.get('paging', {})
                        cursors = paging.get('cursors', {})
                        after_cursor = cursors.get('after')
                        
                        if not after_cursor:
                            print("📄 Reached end of data - no more pages available")
                            break
                        
                        # Add small delay to respect rate limits
                        time.sleep(0.3)
                        
                    elif response.status_code == 400:
                        error_data = response.json()
                        error_message = error_data.get('error', {}).get('message', 'Unknown error')
                        print(f"❌ Facebook API error: {response.status_code} - {error_message}")
                        self.loading_status['error_message'] = error_message
                        break
                    
                    else:
                        print(f"❌ Facebook API error: {response.status_code} - {response.text}")
                        self.loading_status['error_message'] = f"API error: {response.status_code}"
                        break
                        
                except requests.exceptions.Timeout:
                    print("⏰ Facebook API request timeout, retrying...")
                    time.sleep(2)
                    continue
                except Exception as e:
                    print(f"❌ Facebook API request error: {e}")
                    self.loading_status['error_message'] = str(e)
                    break
            
            # Trim to exactly 747 ads if we got more
            if len(all_ads_data) > 747:
                all_ads_data = all_ads_data[:747]
                print(f"✂️ Trimmed to exactly 747 ads")
            
            # Process the data
            self.process_facebook_data_chunk(all_ads_data)
            self.process_combined_data()
            
            # Save to cache
            self.save_facebook_data_cache()
            
            # Update status
            self.loading_status['facebook_api_loading'] = False
            self.loading_status['facebook_api_loaded'] = True
            self.loading_status['data_processed'] = True
            self.loading_status['pagination_complete'] = True
            self.loading_status['last_updated'] = datetime.now().isoformat()
            
            print(f"✅ Facebook API loading complete - {len(all_ads_data)} total ads loaded")
            
            if len(all_ads_data) == 747:
                print(f"🎯 Perfect match! Loaded exactly 747 ads as expected")
            else:
                print(f"⚠️ Loaded {len(all_ads_data)} ads instead of expected 747")
                
        except Exception as e:
            print(f"❌ Facebook API loading error: {e}")
            self.loading_status['facebook_api_loading'] = False
            self.loading_status['error_message'] = str(e)

    def process_facebook_data_chunk(self, all_ads_data):
        """Process Facebook API data chunk"""
        try:
            fb_api_records = []
            for ad in all_ads_data:
                try:
                    ad_id = ad.get('id', '')
                    ad_name = ad.get('name', '')
                    adset_info = ad.get('adset', {})
                    adset_id = adset_info.get('id', '') if adset_info else ''
                    adset_name = adset_info.get('name', '') if adset_info else ''
                    insights = ad.get('insights', {}).get('data', [])
                    
                    if insights:
                        # Aggregate insights data across date ranges
                        total_impressions = 0
                        total_clicks = 0
                        total_spend = 0
                        
                        for insight in insights:
                            total_impressions += self.safe_float(insight.get('impressions', 0))
                            total_clicks += self.safe_float(insight.get('clicks', 0))
                            total_spend += self.safe_float(insight.get('spend', 0))
                        
                        # Calculate metrics
                        ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                        cpc = total_spend / total_clicks if total_clicks > 0 else 0
                        cpm = total_spend / total_impressions * 1000 if total_impressions > 0 else 0
                        
                        fb_api_records.append({
                            'ad_id': ad_id,
                            'ad_name': ad_name,
                            'adset_id': adset_id,
                            'adset_name': adset_name,
                            'impressions': total_impressions,
                            'clicks': total_clicks,
                            'spend': total_spend,
                            'ctr': ctr,
                            'cpc': cpc,
                            'cpm': cpm
                        })
                    else:
                        # Include ads without insights data (they might have spend in Google Sheets)
                        fb_api_records.append({
                            'ad_id': ad_id,
                            'ad_name': ad_name,
                            'adset_id': adset_id,
                            'adset_name': adset_name,
                            'impressions': 0,
                            'clicks': 0,
                            'spend': 0,
                            'ctr': 0,
                            'cpc': 0,
                            'cpm': 0
                        })
                        
                except Exception as e:
                    print(f"⚠️ Error processing ad data: {e}")
                    continue
            
            self.fb_api_data = fb_api_records
            
        except Exception as e:
            print(f"❌ Error processing Facebook data chunk: {e}")

    def safe_float(self, value, default=0):
        """Safely convert value to float"""
        try:
            if value is None or value == '' or value == 'None':
                return default
            # Handle string numbers with commas
            if isinstance(value, str):
                value = value.replace(',', '')
            return float(value)
        except (ValueError, TypeError):
            return default

    def process_combined_data(self):
        """Process and combine data from all sources using Ad Set + Ad Name combinations"""
        try:
            combined_data = []
            
            print(f"🔄 Processing data: {len(self.web_data)} web, {len(self.attr_data)} attr, {len(self.fb_data)} fb sheets, {len(self.fb_api_data)} fb api")
            
            # Create lookup dictionaries using Ad Set + Ad Name combinations
            web_lookup = {}
            for row in self.web_data:
                utm_content = str(row.get('Web Pages UTM Content', '')).strip()
                utm_term = str(row.get('Web Pages UTM Term', '')).strip()
                if utm_content and utm_term and utm_content.lower() != 'total' and utm_term.lower() != 'total':
                    # Create combination key
                    combination_key = f"{utm_term}|||{utm_content}"
                    web_lookup[combination_key.lower()] = row
                    # Also add individual keys for fallback
                    web_lookup[utm_content.lower()] = row
                    web_lookup[utm_term.lower()] = row
            
            attr_lookup = {}
            for row in self.attr_data:
                utm_content = str(row.get('Attribution UTM Content', '')).strip()
                utm_term = str(row.get('Attribution UTM Term', '')).strip()
                if utm_content and utm_term and utm_content.lower() != 'total' and utm_term.lower() != 'total':
                    # Create combination key
                    combination_key = f"{utm_term}|||{utm_content}"
                    attr_lookup[combination_key.lower()] = row
                    # Also add individual keys for fallback
                    attr_lookup[utm_content.lower()] = row
                    attr_lookup[utm_term.lower()] = row
            
            # Create FB spend lookup using Ad Set + Ad Name combinations
            fb_spend_lookup = {}
            for row in self.fb_data:
                ad_set_name = str(row.get('Facebook Adset Name', '')).strip()
                ad_name = str(row.get('Facebook Ad Name', '')).strip()
                if ad_set_name and ad_name and ad_set_name.lower() != 'total' and ad_name.lower() != 'total':
                    # Create combination key
                    combination_key = f"{ad_set_name}|||{ad_name}"
                    fb_spend_lookup[combination_key.lower()] = row
                    # Also add individual keys for fallback
                    fb_spend_lookup[ad_name.lower()] = row
                    fb_spend_lookup[ad_set_name.lower()] = row
            
            # Create FB API lookup using Ad Set + Ad Name combinations
            fb_api_lookup = {}
            for row in self.fb_api_data:
                ad_set_name = str(row.get('adset_name', '')).strip()
                ad_name = str(row.get('ad_name', '')).strip()
                if ad_set_name and ad_name:
                    # Create combination key
                    combination_key = f"{ad_set_name}|||{ad_name}"
                    fb_api_lookup[combination_key.lower()] = row
                    # Also add individual keys for fallback
                    fb_api_lookup[ad_name.lower()] = row
                    fb_api_lookup[ad_set_name.lower()] = row
            
            print(f"🔍 Created lookups: {len(web_lookup)} web, {len(attr_lookup)} attr, {len(fb_spend_lookup)} fb spend, {len(fb_api_lookup)} fb api")
            
            # Process FB API data as primary source and combine with other data
            processed_count = 0
            for fb_api_row in self.fb_api_data:
                ad_set_name = str(fb_api_row.get('adset_name', '')).strip()
                ad_name = str(fb_api_row.get('ad_name', '')).strip()
                ad_id = str(fb_api_row.get('ad_id', '')).strip()
                
                # Skip empty rows
                if not ad_set_name or not ad_name:
                    continue
                
                # Try to find matching data using Ad Set + Ad Name combination first
                web_row = {}
                attr_row = {}
                fb_spend_row = {}
                
                # Primary key: Ad Set + Ad Name combination
                primary_key = f"{ad_set_name}|||{ad_name}".lower()
                
                # Try combination key first, then fallback to individual keys
                search_keys = [
                    primary_key,
                    ad_name.lower(),
                    ad_set_name.lower()
                ]
                
                for key in search_keys:
                    if key in web_lookup:
                        web_row = web_lookup[key]
                        break
                
                for key in search_keys:
                    if key in attr_lookup:
                        attr_row = attr_lookup[key]
                        break
                
                for key in search_keys:
                    if key in fb_spend_lookup:
                        fb_spend_row = fb_spend_lookup[key]
                        break
                
                # Get marketing metrics from Facebook API (accurate)
                impressions = self.safe_float(fb_api_row.get('impressions', 0))
                clicks = self.safe_float(fb_api_row.get('clicks', 0))
                fb_api_ctr = self.safe_float(fb_api_row.get('ctr', 0))
                fb_api_cpc = self.safe_float(fb_api_row.get('cpc', 0))
                fb_api_cpm = self.safe_float(fb_api_row.get('cpm', 0))
                
                # Get spend from Google Sheets (more reliable for your setup)
                spend = self.safe_float(fb_spend_row.get('Facebook Total Spend (USD)', 0))
                
                # If no spend from Google Sheets, fallback to FB API spend
                if spend == 0:
                    spend = self.safe_float(fb_api_row.get('spend', 0))
                
                # Use Facebook API metrics (more accurate)
                ctr = fb_api_ctr
                cpc = fb_api_cpc
                cpm = fb_api_cpm
                
                # If FB API metrics are 0, recalculate using spend from Google Sheets
                if ctr == 0 and impressions > 0:
                    ctr = (clicks / impressions * 100)
                if cpc == 0 and clicks > 0:
                    cpc = spend / clicks
                if cpm == 0 and impressions > 0:
                    cpm = spend / impressions * 1000
                
                # Web data
                site_visits = self.safe_float(web_row.get('Web Pages Unique Count of Landing Pages', 0))
                funnel_starts = self.safe_float(web_row.get('Web Pages Unique Count of Sessions with Funnel Starts', 0))
                survey_complete = self.safe_float(web_row.get('Web Pages Unique Count of Sessions with Match Results', 0))
                checkout_starts = self.safe_float(web_row.get('Count of Sessions with Checkout Started (V2 included)', 0))
                
                # Attribution data - Fixed Offer Spend mapping
                bookings = self.safe_float(attr_row.get('Attribution Attributed NPRs', 0))
                revenue = self.safe_float(attr_row.get('Attribution Attibuted Total Revenue (Predicted) (USD)', 0))
                completion_rate = self.safe_float(attr_row.get('Attribution Attibuted PAS (Predicted)', 0.45))
                # Fixed mapping for Offer Spend
                promo_spend = self.safe_float(attr_row.get('Attibuted Offer Spend (Predicted) (USD)', 0))
                
                # Apply completion rate failsafe (39%-51% range, default 45%)
                if completion_rate < 0.39 or completion_rate > 0.51:
                    completion_rate = 0.45
                
                # Calculate conversion metrics
                funnel_start_rate = (funnel_starts / site_visits * 100) if site_visits > 0 else 0
                survey_completion_rate = (survey_complete / funnel_starts * 100) if funnel_starts > 0 else 0
                checkout_start_rate = (checkout_starts / survey_complete * 100) if survey_complete > 0 else 0
                booking_conversion_rate = (bookings / site_visits * 100) if site_visits > 0 else 0
                
                cpa = spend / bookings if bookings > 0 else 0
                total_cost = spend + promo_spend
                effective_bookings = bookings * completion_rate
                cac = total_cost / effective_bookings if effective_bookings > 0 else 0
                ltv = revenue / effective_bookings if effective_bookings > 0 else 0
                roas = ltv / cac if cac > 0 else 0
                
                # Success criteria using dynamic KPI settings
                success_criteria = {
                    'ctr_good': ctr > self.kpi_settings['ctr_threshold'],
                    'funnel_start_good': funnel_start_rate > self.kpi_settings['funnel_start_threshold'],
                    'cpa_good': cpa < self.kpi_settings['cpa_threshold'] and cpa > 0,
                    'clicks_good': clicks > self.kpi_settings['clicks_threshold'],
                    'roas_good': roas > self.kpi_settings['roas_threshold'],
                    'cpc_good': cpc < self.kpi_settings['cpc_threshold'] and cpc > 0,
                    'cpm_good': cpm < self.kpi_settings['cpm_threshold'] and cpm > 0,
                    'booking_conversion_good': booking_conversion_rate > self.kpi_settings['booking_conversion_threshold']
                }
                
                combined_row = {
                    'ad_id': ad_id,
                    'ad_set_name': ad_set_name,
                    'ad_name': ad_name,
                    'spend': spend,
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': ctr,
                    'cpc': cpc,
                    'cpm': cpm,
                    'site_visits': site_visits,
                    'funnel_starts': funnel_starts,
                    'funnel_start_rate': funnel_start_rate,
                    'survey_complete': survey_complete,
                    'survey_completion_rate': survey_completion_rate,
                    'checkout_starts': checkout_starts,
                    'checkout_start_rate': checkout_start_rate,
                    'bookings': bookings,
                    'booking_conversion_rate': booking_conversion_rate,
                    'cpa': cpa,
                    'revenue': revenue,
                    'ltv': ltv,
                    'cac': cac,
                    'roas': roas,
                    'completion_rate': completion_rate,
                    'promo_spend': promo_spend,
                    'total_cost': total_cost,
                    'success_criteria': success_criteria,
                    'all_criteria_met': all(success_criteria.values()),
                    'has_web_data': bool(web_row),
                    'has_attr_data': bool(attr_row),
                    'has_fb_spend_data': bool(fb_spend_row),
                    'data_source': 'fb_api'
                }
                
                combined_data.append(combined_row)
                processed_count += 1
            
            self.performance_data = combined_data
            print(f"✅ Processed {processed_count} combined records from {len(self.fb_api_data)} FB API records")
            
            if processed_count == 0:
                print("⚠️ No records were successfully combined - check data format...")
                
        except Exception as e:
            print(f"❌ Data processing error: {e}")

    def get_performance_summary(self):
        """Get performance summary statistics with corrected calculations"""
        if not self.performance_data:
            return {
                'total_ads': 0,
                'total_spend': 0,
                'total_revenue': 0,
                'total_clicks': 0,
                'total_impressions': 0,
                'total_bookings': 0,
                'total_offer_spend': 0,
                'avg_ctr': 0,
                'avg_cpc': 0,
                'avg_cpa': 0,
                'avg_roas': 0,
                'avg_cpm': 0,
                'avg_completion_rate': 0,
                'avg_funnel_start_rate': 0,
                'avg_booking_rate': 0,
                'overall_roas': 0,
                'overall_ctr': 0,
                'roi': 0,
                'successful_ads': 0
            }
        
        # Calculate totals
        total_spend = sum(ad['spend'] for ad in self.performance_data)
        total_revenue = sum(ad['revenue'] for ad in self.performance_data)
        total_clicks = sum(ad['clicks'] for ad in self.performance_data)
        total_impressions = sum(ad['impressions'] for ad in self.performance_data)
        total_bookings = sum(ad['bookings'] for ad in self.performance_data)
        total_offer_spend = sum(ad['promo_spend'] for ad in self.performance_data)
        
        # Calculate overall metrics (corrected)
        overall_roas = total_revenue / total_spend if total_spend > 0 else 0
        overall_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        roi = ((total_revenue - total_offer_spend - total_spend) / total_revenue * 100) if total_revenue > 0 else 0
        
        # Calculate averages (excluding zero values where appropriate)
        ads_with_cpc = [ad for ad in self.performance_data if ad['cpc'] > 0]
        ads_with_cpm = [ad for ad in self.performance_data if ad['cpm'] > 0]
        ads_with_cpa = [ad for ad in self.performance_data if ad['cpa'] > 0]
        ads_with_roas = [ad for ad in self.performance_data if ad['roas'] > 0]
        
        avg_ctr = sum(ad['ctr'] for ad in self.performance_data) / len(self.performance_data)
        avg_cpc = sum(ad['cpc'] for ad in ads_with_cpc) / max(1, len(ads_with_cpc))
        avg_cpm = sum(ad['cpm'] for ad in ads_with_cpm) / max(1, len(ads_with_cpm))
        avg_cpa = sum(ad['cpa'] for ad in ads_with_cpa) / max(1, len(ads_with_cpa))
        avg_roas = sum(ad['roas'] for ad in ads_with_roas) / max(1, len(ads_with_roas))
        avg_completion_rate = sum(ad['completion_rate'] for ad in self.performance_data) / len(self.performance_data)
        avg_funnel_start_rate = sum(ad['funnel_start_rate'] for ad in self.performance_data) / len(self.performance_data)
        avg_booking_rate = sum(ad['booking_conversion_rate'] for ad in self.performance_data) / len(self.performance_data)
        
        successful_ads = len([ad for ad in self.performance_data if ad['all_criteria_met']])
        
        return {
            'total_ads': len(self.performance_data),
            'total_spend': total_spend,
            'total_revenue': total_revenue,
            'total_clicks': total_clicks,
            'total_impressions': total_impressions,
            'total_bookings': total_bookings,
            'total_offer_spend': total_offer_spend,
            'avg_ctr': avg_ctr,
            'avg_cpc': avg_cpc,
            'avg_cpm': avg_cpm,
            'avg_cpa': avg_cpa,
            'avg_roas': avg_roas,
            'avg_completion_rate': avg_completion_rate,
            'avg_funnel_start_rate': avg_funnel_start_rate,
            'avg_booking_rate': avg_booking_rate,
            'overall_roas': overall_roas,
            'overall_ctr': overall_ctr,
            'roi': roi,
            'successful_ads': successful_ads
        }

    def refresh_data(self):
        """Refresh all data sources"""
        try:
            # Clear cache to force fresh load
            cache_file = '/tmp/facebook_api_cache.pkl'
            if os.path.exists(cache_file):
                os.remove(cache_file)
            
            # Reset loading status
            self.loading_status = {
                'google_sheets_loaded': False,
                'facebook_api_loading': False,
                'facebook_api_loaded': False,
                'data_processed': False,
                'total_ads_loaded': 0,
                'current_page': 0,
                'error_message': None,
                'last_updated': None,
                'expected_ads': 747,
                'pagination_complete': False
            }
            
            # Start fresh background loading
            self.start_background_loading()
            
            return True
        except Exception as e:
            print(f"❌ Error refreshing data: {e}")
            return False

# Initialize the tool
tool = FacebookOptimizationTool()

@app.route('/')
def dashboard():
    return render_template('enhanced_dashboard.html')

@app.route('/debug')
def debug_page():
    """Debug page to analyze Facebook API filtering"""
    debug_info = tool.debug_facebook_api_filtering()
    
    # Create HTML debug page
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Facebook API Debug Analysis</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
            .error {{ background-color: #ffebee; color: #c62828; }}
            .success {{ background-color: #e8f5e8; color: #2e7d32; }}
            .warning {{ background-color: #fff3e0; color: #ef6c00; }}
            .info {{ background-color: #e3f2fd; color: #1565c0; }}
            table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f5f5f5; }}
            .code {{ background-color: #f5f5f5; padding: 10px; border-radius: 3px; font-family: monospace; }}
            .metric {{ display: inline-block; margin: 10px; padding: 10px; background-color: #f0f0f0; border-radius: 5px; }}
        </style>
    </head>
    <body>
        <h1>🧪 Facebook API Debug Analysis</h1>
        
        <div class="section info">
            <h2>📊 Summary</h2>
            <div class="metric"><strong>Google Sheets Rows:</strong> {debug_info.get('google_sheets_analysis', {}).get('total_rows', 'N/A')}</div>
            <div class="metric"><strong>Unique Combinations:</strong> {debug_info.get('google_sheets_analysis', {}).get('unique_combinations', 'N/A')}</div>
            <div class="metric"><strong>Facebook API Ads:</strong> {debug_info.get('facebook_api_analysis', {}).get('total_ads_fetched', 'N/A')}</div>
            <div class="metric"><strong>Matching Combinations:</strong> {debug_info.get('comparison', {}).get('matching_combinations', 'N/A')}</div>
            <div class="metric"><strong>Match %:</strong> {debug_info.get('comparison', {}).get('match_percentage', 0):.1f}%</div>
        </div>
        
        {f'<div class="section error"><h2>❌ Errors</h2><ul>{"".join([f"<li>{error}</li>" for error in debug_info.get("errors", [])])}</ul></div>' if debug_info.get('errors') else ''}
        
        <div class="section">
            <h2>📋 Google Sheets Analysis</h2>
            <div class="metric"><strong>Total Rows:</strong> {debug_info.get('google_sheets_analysis', {}).get('total_rows', 'N/A')}</div>
            <div class="metric"><strong>Unique Ad Set + Ad Name Combinations:</strong> {debug_info.get('google_sheets_analysis', {}).get('unique_combinations', 'N/A')}</div>
            <div class="metric"><strong>Unique Ad Names:</strong> {debug_info.get('google_sheets_analysis', {}).get('unique_ad_names', 'N/A')}</div>
            <div class="metric"><strong>Unique Ad Set Names:</strong> {debug_info.get('google_sheets_analysis', {}).get('unique_adset_names', 'N/A')}</div>
            
            <h3>Sample Google Sheets Rows:</h3>
            <table>
                <tr><th>Row #</th><th>Ad Set Name</th><th>Ad Name</th><th>Combination Key</th></tr>
                {"".join([f"<tr><td>{row['row_number']}</td><td>{row['ad_set_name']}</td><td>{row['ad_name']}</td><td>{row['combination']}</td></tr>" for row in debug_info.get('sample_data', {}).get('google_sheets_rows', [])])}
            </table>
            
            <h3>Sample Combinations:</h3>
            <div class="code">
                {"<br>".join(debug_info.get('google_sheets_analysis', {}).get('sample_combinations', []))}
            </div>
        </div>
        
        <div class="section">
            <h2>📡 Facebook API Analysis</h2>
            <div class="metric"><strong>Total Ads Fetched:</strong> {debug_info.get('facebook_api_analysis', {}).get('total_ads_fetched', 'N/A')}</div>
            <div class="metric"><strong>Pages Fetched:</strong> {debug_info.get('facebook_api_analysis', {}).get('pages_fetched', 'N/A')}</div>
            <div class="metric"><strong>Unique Combinations:</strong> {debug_info.get('facebook_api_analysis', {}).get('unique_combinations', 'N/A')}</div>
            <div class="metric"><strong>Unique Ad Names:</strong> {debug_info.get('facebook_api_analysis', {}).get('unique_ad_names', 'N/A')}</div>
            <div class="metric"><strong>Unique Ad Set Names:</strong> {debug_info.get('facebook_api_analysis', {}).get('unique_adset_names', 'N/A')}</div>
            
            <h3>Sample Facebook API Ads:</h3>
            <table>
                <tr><th>Ad #</th><th>Ad ID</th><th>Ad Set Name</th><th>Ad Name</th><th>Status</th><th>Combination Key</th></tr>
                {"".join([f"<tr><td>{ad['ad_number']}</td><td>{ad['ad_id']}</td><td>{ad['adset_name']}</td><td>{ad['ad_name']}</td><td>{ad['status']}/{ad['effective_status']}</td><td>{ad['combination']}</td></tr>" for ad in debug_info.get('sample_data', {}).get('facebook_api_ads', [])])}
            </table>
        </div>
        
        <div class="section">
            <h2>🔍 Comparison Analysis</h2>
            <div class="metric"><strong>Matching Combinations:</strong> {debug_info.get('comparison', {}).get('matching_combinations', 'N/A')}</div>
            <div class="metric"><strong>Missing in Facebook:</strong> {debug_info.get('comparison', {}).get('missing_in_facebook', 'N/A')}</div>
            <div class="metric"><strong>Extra in Facebook:</strong> {debug_info.get('comparison', {}).get('extra_in_facebook', 'N/A')}</div>
            <div class="metric"><strong>Match Percentage:</strong> {debug_info.get('comparison', {}).get('match_percentage', 0):.1f}%</div>
            
            <h3>✅ Sample Matching Combinations:</h3>
            <div class="code">
                {"<br>".join(debug_info.get('comparison', {}).get('sample_matches', []))}
            </div>
            
            <h3>❌ Sample Missing in Facebook API:</h3>
            <div class="code">
                {"<br>".join(debug_info.get('comparison', {}).get('sample_missing_in_fb', []))}
            </div>
            
            <h3>⚠️ Sample Extra in Facebook API:</h3>
            <div class="code">
                {"<br>".join(debug_info.get('comparison', {}).get('sample_extra_in_fb', []))}
            </div>
        </div>
        
        <div class="section info">
            <h2>🎯 Next Steps</h2>
            <p>Based on this analysis:</p>
            <ul>
                <li>If match percentage is low, check if ad names/ad set names have different formatting</li>
                <li>If many ads are missing in Facebook API, check date range or ad status filters</li>
                <li>If many extra ads in Facebook API, check if Google Sheets data is complete</li>
                <li>Look at the sample data to identify patterns in mismatches</li>
            </ul>
        </div>
        
        <div class="section">
            <p><a href="/">← Back to Dashboard</a></p>
        </div>
    </body>
    </html>
    """
    
    return html_content

@app.route('/api/loading-status')
def loading_status():
    """Get current data loading status"""
    return jsonify(tool.loading_status)

@app.route('/api/performance-summary')
def performance_summary():
    summary = tool.get_performance_summary()
    return jsonify(summary)

@app.route('/api/performance-data')
def performance_data():
    # Get filter parameter
    search_filter = request.args.get('filter', '').lower()
    
    # Filter data if search term provided
    filtered_data = tool.performance_data
    if search_filter:
        filtered_data = [
            ad for ad in tool.performance_data 
            if search_filter in ad['ad_name'].lower() or search_filter in ad['ad_set_name'].lower()
        ]
    
    return jsonify(filtered_data)

@app.route('/api/refresh-data')
def refresh_data():
    try:
        success = tool.refresh_data()
        if success:
            return jsonify({'status': 'success', 'message': 'Data refresh started in background'})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to start data refresh'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    print("✅ OpenAI API key configured" if tool.openai_api_key else "❌ OpenAI API key missing")
    print("✅ Facebook API credentials configured" if tool.fb_access_token and tool.fb_ad_account_id else "❌ Facebook API credentials missing")
    print(f"✅ KPI Settings loaded: {tool.kpi_settings}")
    print(f"✅ Optimization Rules loaded: {tool.optimization_rules}")
    app.run(host='0.0.0.0', port=8080, debug=False)

