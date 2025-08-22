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
            'facebook_creative_loading': False,
            'facebook_creative_loaded': False,
            'data_processed': False,
            'error_message': None,
            'last_updated': None
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
        self.creative_data = {}  # For Facebook creative intelligence
        
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
                # Load Google Sheets data (primary source)
                self.load_google_sheets_data()
                self.loading_status['google_sheets_loaded'] = True
                
                # Process combined data using Google Sheets only
                self.process_combined_data()
                self.loading_status['data_processed'] = True
                
                # Load Facebook creative data in background (for AI insights)
                self.load_facebook_creative_data_background()
                
                self.loading_status['last_updated'] = datetime.now().isoformat()
                
            except Exception as e:
                print(f"❌ Background loading error: {e}")
                self.loading_status['error_message'] = str(e)
        
        # Start background thread
        thread = threading.Thread(target=background_load, daemon=True)
        thread.start()

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
            
            # FB Spend Data (primary source for performance metrics)
            fb_sheet = self.gc.open_by_url('https://docs.google.com/spreadsheets/d/1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk/edit?usp=sharing')
            fb_worksheet = fb_sheet.get_worksheet(0)
            fb_records = fb_worksheet.get_all_records()
            self.fb_data = fb_records
            print(f"✅ Loaded {len(fb_records)} rows from fb_spend")
            
        except Exception as e:
            print(f"❌ Google Sheets loading error: {e}")

    def load_facebook_creative_data_background(self):
        """Load Facebook creative data for AI insights (separate from performance data)"""
        try:
            if not self.fb_access_token or not self.fb_ad_account_id:
                print("⚠️ Facebook API credentials not available for creative data")
                return
            
            self.loading_status['facebook_creative_loading'] = True
            
            print("🎨 Loading Facebook creative data for AI insights...")
            
            # Load ad set level creative data
            adset_data = self.load_facebook_adsets_creative()
            
            # Load ad level creative data
            ad_data = self.load_facebook_ads_creative()
            
            self.creative_data = {
                'adsets': adset_data,
                'ads': ad_data,
                'last_updated': datetime.now().isoformat()
            }
            
            self.loading_status['facebook_creative_loading'] = False
            self.loading_status['facebook_creative_loaded'] = True
            
            print(f"✅ Facebook creative data loaded: {len(adset_data)} ad sets, {len(ad_data)} ads")
            
        except Exception as e:
            print(f"❌ Facebook creative data loading error: {e}")
            self.loading_status['facebook_creative_loading'] = False
            self.loading_status['error_message'] = str(e)

    def load_facebook_adsets_creative(self):
        """Load ad set level creative data from Facebook API"""
        try:
            url = f"https://graph.facebook.com/v18.0/{self.fb_ad_account_id}/adsets"
            
            params = {
                'access_token': self.fb_access_token,
                'fields': 'id,name,targeting,optimization_goal,billing_event,bid_amount,daily_budget,lifetime_budget,start_time,end_time,status,effective_status',
                'limit': 100
            }
            
            adsets_data = []
            after_cursor = None
            page_count = 0
            
            while page_count < 20:  # Limit to prevent timeout
                if after_cursor:
                    params['after'] = after_cursor
                
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    adsets = data.get('data', [])
                    
                    if not adsets:
                        break
                    
                    adsets_data.extend(adsets)
                    page_count += 1
                    
                    # Check for next page
                    paging = data.get('paging', {})
                    cursors = paging.get('cursors', {})
                    after_cursor = cursors.get('after')
                    
                    if not after_cursor:
                        break
                        
                    time.sleep(0.2)  # Rate limit respect
                    
                else:
                    print(f"⚠️ Facebook adsets API error: {response.status_code}")
                    break
            
            return adsets_data
            
        except Exception as e:
            print(f"❌ Error loading Facebook adsets: {e}")
            return []

    def load_facebook_ads_creative(self):
        """Load ad level creative data from Facebook API"""
        try:
            url = f"https://graph.facebook.com/v18.0/{self.fb_ad_account_id}/ads"
            
            params = {
                'access_token': self.fb_access_token,
                'fields': 'id,name,adset{id,name},creative{id,title,body,image_url,video_id,url_tags,object_story_spec},status,effective_status',
                'limit': 100
            }
            
            ads_data = []
            after_cursor = None
            page_count = 0
            
            while page_count < 20:  # Limit to prevent timeout
                if after_cursor:
                    params['after'] = after_cursor
                
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    ads = data.get('data', [])
                    
                    if not ads:
                        break
                    
                    # Process creative data
                    for ad in ads:
                        creative = ad.get('creative', {})
                        if creative:
                            # Extract creative elements
                            ad['headline'] = creative.get('title', '')
                            ad['text'] = creative.get('body', '')
                            ad['image_url'] = creative.get('image_url', '')
                            ad['video_id'] = creative.get('video_id', '')
                            
                            # Extract landing page URL from object_story_spec
                            object_story = creative.get('object_story_spec', {})
                            link_data = object_story.get('link_data', {})
                            ad['landing_page_url'] = link_data.get('link', '')
                            ad['call_to_action'] = link_data.get('call_to_action', {}).get('type', '')
                    
                    ads_data.extend(ads)
                    page_count += 1
                    
                    # Check for next page
                    paging = data.get('paging', {})
                    cursors = paging.get('cursors', {})
                    after_cursor = cursors.get('after')
                    
                    if not after_cursor:
                        break
                        
                    time.sleep(0.2)  # Rate limit respect
                    
                else:
                    print(f"⚠️ Facebook ads API error: {response.status_code}")
                    break
            
            return ads_data
            
        except Exception as e:
            print(f"❌ Error loading Facebook ads: {e}")
            return []

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
        """Process and combine data from Google Sheets sources only"""
        try:
            combined_data = []
            
            print(f"🔄 Processing Google Sheets data: {len(self.web_data)} web, {len(self.attr_data)} attr, {len(self.fb_data)} fb")
            
            # Create lookup dictionaries
            web_lookup = {}
            for row in self.web_data:
                utm_content = str(row.get('Web Pages UTM Content', '')).strip()
                utm_term = str(row.get('Web Pages UTM Term', '')).strip()
                if utm_content and utm_term and utm_content.lower() != 'total' and utm_term.lower() != 'total':
                    # Create multiple keys for flexible matching
                    keys = [
                        f"{utm_term}|||{utm_content}".lower(),
                        utm_content.lower(),
                        utm_term.lower()
                    ]
                    for key in keys:
                        web_lookup[key] = row
            
            attr_lookup = {}
            for row in self.attr_data:
                utm_content = str(row.get('Attribution UTM Content', '')).strip()
                utm_term = str(row.get('Attribution UTM Term', '')).strip()
                if utm_content and utm_term and utm_content.lower() != 'total' and utm_term.lower() != 'total':
                    # Create multiple keys for flexible matching
                    keys = [
                        f"{utm_term}|||{utm_content}".lower(),
                        utm_content.lower(),
                        utm_term.lower()
                    ]
                    for key in keys:
                        attr_lookup[key] = row
            
            print(f"🔍 Created lookups: {len(web_lookup)} web, {len(attr_lookup)} attr")
            
            # Process FB data as primary source
            processed_count = 0
            for fb_row in self.fb_data:
                ad_set_name = str(fb_row.get('Facebook Adset Name', '')).strip()
                ad_name = str(fb_row.get('Facebook Ad Name', '')).strip()
                
                # Skip empty or total rows
                if (not ad_set_name or not ad_name or 
                    ad_set_name.lower() == 'total' or ad_name.lower() == 'total'):
                    continue
                
                # Try to find matching data using multiple keys
                web_row = {}
                attr_row = {}
                
                # Search keys in order of preference
                search_keys = [
                    f"{ad_set_name}|||{ad_name}".lower(),
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
                
                # Get spend and impressions from Facebook data
                spend = self.safe_float(fb_row.get('Facebook Total Spend (USD)', 0))
                impressions = self.safe_float(fb_row.get('Facebook Total Impressions', 0))
                
                # Calculate Link Clicks using Web Pages data (more accurate)
                site_visits = self.safe_float(web_row.get('Web Pages Unique Count of Landing Pages', 0))
                link_clicks = site_visits * 0.9  # Estimate link clicks as 90% of site visits
                
                # Calculate marketing metrics using corrected link clicks
                ctr = (link_clicks / impressions * 100) if impressions > 0 else 0
                cpc = spend / link_clicks if link_clicks > 0 else 0
                cpm = spend / impressions * 1000 if impressions > 0 else 0
                
                # Web data
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
                    'clicks_good': link_clicks > self.kpi_settings['clicks_threshold'],
                    'roas_good': roas > self.kpi_settings['roas_threshold'],
                    'cpc_good': cpc < self.kpi_settings['cpc_threshold'] and cpc > 0,
                    'cpm_good': cpm < self.kpi_settings['cpm_threshold'] and cpm > 0,
                    'booking_conversion_good': booking_conversion_rate > self.kpi_settings['booking_conversion_threshold']
                }
                
                success_count = sum(1 for criteria in success_criteria.values() if criteria)
                
                combined_row = {
                    'ad_set_name': ad_set_name,
                    'ad_name': ad_name,
                    'spend': spend,
                    'clicks': link_clicks,  # Using calculated link clicks
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
                    'success_count': success_count,
                    'all_criteria_met': success_count == 8,
                    'has_web_data': bool(web_row),
                    'has_attr_data': bool(attr_row),
                    'data_source': 'google_sheets'
                }
                
                combined_data.append(combined_row)
                processed_count += 1
            
            self.performance_data = combined_data
            print(f"✅ Processed {processed_count} combined records from Google Sheets data")
            
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

    def get_creative_dashboard_data(self):
        """Get ad-level grouped data for Creative Dashboard"""
        if not self.performance_data:
            return []
        
        # Group by ad name only
        ad_groups = defaultdict(list)
        for ad in self.performance_data:
            ad_groups[ad['ad_name']].append(ad)
        
        creative_data = []
        for ad_name, ads in ad_groups.items():
            # Aggregate metrics across all instances of this ad
            total_spend = sum(ad['spend'] for ad in ads)
            total_clicks = sum(ad['clicks'] for ad in ads)
            total_impressions = sum(ad['impressions'] for ad in ads)
            total_bookings = sum(ad['bookings'] for ad in ads)
            total_revenue = sum(ad['revenue'] for ad in ads)
            total_promo_spend = sum(ad['promo_spend'] for ad in ads)
            total_site_visits = sum(ad['site_visits'] for ad in ads)
            total_funnel_starts = sum(ad['funnel_starts'] for ad in ads)
            
            # Calculate aggregated metrics
            ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
            cpc = total_spend / total_clicks if total_clicks > 0 else 0
            cpm = total_spend / total_impressions * 1000 if total_impressions > 0 else 0
            cpa = total_spend / total_bookings if total_bookings > 0 else 0
            roas = total_revenue / total_spend if total_spend > 0 else 0
            funnel_start_rate = (total_funnel_starts / total_site_visits * 100) if total_site_visits > 0 else 0
            booking_conversion_rate = (total_bookings / total_site_visits * 100) if total_site_visits > 0 else 0
            
            # Average completion rate
            avg_completion_rate = sum(ad['completion_rate'] for ad in ads) / len(ads)
            
            # Success criteria
            success_criteria = {
                'ctr_good': ctr > self.kpi_settings['ctr_threshold'],
                'funnel_start_good': funnel_start_rate > self.kpi_settings['funnel_start_threshold'],
                'cpa_good': cpa < self.kpi_settings['cpa_threshold'] and cpa > 0,
                'clicks_good': total_clicks > self.kpi_settings['clicks_threshold'],
                'roas_good': roas > self.kpi_settings['roas_threshold'],
                'cpc_good': cpc < self.kpi_settings['cpc_threshold'] and cpc > 0,
                'cpm_good': cpm < self.kpi_settings['cpm_threshold'] and cpm > 0,
                'booking_conversion_good': booking_conversion_rate > self.kpi_settings['booking_conversion_threshold']
            }
            
            success_count = sum(1 for criteria in success_criteria.values() if criteria)
            
            creative_data.append({
                'ad_name': ad_name,
                'ad_count': len(ads),
                'spend': total_spend,
                'clicks': total_clicks,
                'impressions': total_impressions,
                'ctr': ctr,
                'cpc': cpc,
                'cpm': cpm,
                'funnel_start_rate': funnel_start_rate,
                'booking_conversion_rate': booking_conversion_rate,
                'completion_rate': avg_completion_rate,
                'bookings': total_bookings,
                'cpa': cpa,
                'roas': roas,
                'success_count': success_count,
                'all_criteria_met': success_count == 8
            })
        
        return creative_data

    def get_adgroup_dashboard_data(self):
        """Get ad set level grouped data with nested ads for Ad Group Dashboard"""
        if not self.performance_data:
            return []
        
        # Group by ad set name
        adset_groups = defaultdict(list)
        for ad in self.performance_data:
            adset_groups[ad['ad_set_name']].append(ad)
        
        adgroup_data = []
        for adset_name, ads in adset_groups.items():
            # Calculate success ratio
            successful_ads = len([ad for ad in ads if ad['all_criteria_met']])
            total_ads = len(ads)
            
            # Aggregate ad set metrics
            total_spend = sum(ad['spend'] for ad in ads)
            total_clicks = sum(ad['clicks'] for ad in ads)
            total_impressions = sum(ad['impressions'] for ad in ads)
            total_bookings = sum(ad['bookings'] for ad in ads)
            total_revenue = sum(ad['revenue'] for ad in ads)
            total_promo_spend = sum(ad['promo_spend'] for ad in ads)
            total_site_visits = sum(ad['site_visits'] for ad in ads)
            total_funnel_starts = sum(ad['funnel_starts'] for ad in ads)
            
            # Calculate aggregated metrics
            ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
            cpc = total_spend / total_clicks if total_clicks > 0 else 0
            cpm = total_spend / total_impressions * 1000 if total_impressions > 0 else 0
            cpa = total_spend / total_bookings if total_bookings > 0 else 0
            roas = total_revenue / total_spend if total_spend > 0 else 0
            funnel_start_rate = (total_funnel_starts / total_site_visits * 100) if total_site_visits > 0 else 0
            booking_conversion_rate = (total_bookings / total_site_visits * 100) if total_site_visits > 0 else 0
            
            # Average completion rate
            avg_completion_rate = sum(ad['completion_rate'] for ad in ads) / len(ads)
            
            adgroup_data.append({
                'ad_set_name': adset_name,
                'success_ratio': f"{successful_ads}/{total_ads}",
                'successful_ads': successful_ads,
                'total_ads': total_ads,
                'spend': total_spend,
                'clicks': total_clicks,
                'impressions': total_impressions,
                'ctr': ctr,
                'cpc': cpc,
                'cpm': cpm,
                'funnel_start_rate': funnel_start_rate,
                'booking_conversion_rate': booking_conversion_rate,
                'completion_rate': avg_completion_rate,
                'bookings': total_bookings,
                'cpa': cpa,
                'roas': roas,
                'ads': ads  # Include individual ads for expansion
            })
        
        return adgroup_data

    def get_optimization_recommendations(self):
        """Generate optimization recommendations using configurable rules"""
        if not self.performance_data:
            return []
        
        recommendations = []
        
        for ad in self.performance_data:
            ad_name = ad['ad_name']
            ad_set_name = ad['ad_set_name']
            spend = ad['spend']
            roas = ad['roas']
            cpa = ad['cpa']
            ctr = ad['ctr']
            cpc = ad['cpc']
            bookings = ad['bookings']
            booking_rate = ad['booking_conversion_rate']
            
            # Determine priority based on spend
            if spend >= self.optimization_rules['high_priority_spend_threshold']:
                priority = 'High'
            elif spend >= self.optimization_rules['medium_priority_spend_threshold']:
                priority = 'Medium'
            else:
                priority = 'Low'
            
            # Pause recommendations
            pause_reasons = []
            
            # Rule 1: Low ROAS with significant spend
            if (roas < self.optimization_rules['pause_roas_threshold'] and 
                spend > self.optimization_rules['pause_spend_threshold']):
                pause_reasons.append(f"Low ROAS ({roas:.2f}) with ${spend:.0f} spend")
            
            # Rule 2: High CPA with spend
            if (cpa > self.optimization_rules['pause_cpa_threshold'] and 
                spend > self.optimization_rules['pause_cpa_spend_threshold']):
                pause_reasons.append(f"High CPA (${cpa:.0f}) with ${spend:.0f} spend")
            
            # Rule 3: Low CTR with spend
            if (ctr < self.optimization_rules['pause_ctr_threshold'] and 
                spend > self.optimization_rules['pause_ctr_spend_threshold']):
                pause_reasons.append(f"Low CTR ({ctr:.2f}%) with ${spend:.0f} spend")
            
            # Rule 4: No bookings with significant spend
            if (bookings == 0 and 
                spend > self.optimization_rules['pause_no_bookings_threshold']):
                pause_reasons.append(f"No bookings with ${spend:.0f} spend")
            
            # Rule 5: High CPC with spend
            if (cpc > self.optimization_rules['pause_high_cpc_threshold'] and 
                spend > self.optimization_rules['pause_high_cpc_spend_threshold']):
                pause_reasons.append(f"High CPC (${cpc:.2f}) with ${spend:.0f} spend")
            
            if pause_reasons:
                recommendations.append({
                    'ad_set_name': ad_set_name,
                    'ad_name': ad_name,
                    'action': 'pause',
                    'priority': priority,
                    'reasoning': '; '.join(pause_reasons),
                    'spend': spend,
                    'current_metrics': {
                        'roas': roas,
                        'cpa': cpa,
                        'ctr': ctr,
                        'cpc': cpc,
                        'bookings': bookings
                    }
                })
                continue  # Don't recommend scaling if pausing
            
            # Scale recommendations
            scale_reasons = []
            
            # Rule 1: High ROAS with all criteria met
            if (roas > self.optimization_rules['scale_roas_threshold'] and 
                spend > self.optimization_rules['scale_min_spend_threshold']):
                if self.optimization_rules['scale_all_criteria_required']:
                    if ad['all_criteria_met']:
                        scale_reasons.append(f"High ROAS ({roas:.2f}) with all success criteria met")
                else:
                    scale_reasons.append(f"High ROAS ({roas:.2f})")
            
            # Rule 2: Exceptional CTR
            if ctr > self.optimization_rules['scale_ctr_bonus_threshold']:
                scale_reasons.append(f"Exceptional CTR ({ctr:.2f}%)")
            
            # Rule 3: Low CPA bonus
            if (cpa < self.optimization_rules['scale_cpa_bonus_threshold'] and 
                cpa > 0 and bookings > 0):
                scale_reasons.append(f"Low CPA (${cpa:.0f})")
            
            # Rule 4: High booking rate
            if booking_rate > self.optimization_rules['scale_booking_rate_threshold']:
                scale_reasons.append(f"High booking rate ({booking_rate:.1f}%)")
            
            if scale_reasons:
                recommendations.append({
                    'ad_set_name': ad_set_name,
                    'ad_name': ad_name,
                    'action': 'scale',
                    'priority': priority,
                    'reasoning': '; '.join(scale_reasons),
                    'spend': spend,
                    'current_metrics': {
                        'roas': roas,
                        'cpa': cpa,
                        'ctr': ctr,
                        'cpc': cpc,
                        'bookings': bookings,
                        'booking_rate': booking_rate
                    }
                })
        
        # Sort by priority and spend
        priority_order = {'High': 3, 'Medium': 2, 'Low': 1}
        recommendations.sort(key=lambda x: (priority_order[x['priority']], x['spend']), reverse=True)
        
        return recommendations

    def refresh_data(self):
        """Refresh all data sources"""
        try:
            # Reset loading status
            self.loading_status = {
                'google_sheets_loaded': False,
                'facebook_creative_loading': False,
                'facebook_creative_loaded': False,
                'data_processed': False,
                'error_message': None,
                'last_updated': None
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

@app.route('/api/creative-dashboard-data')
def creative_dashboard_data():
    """Get ad-level grouped data for Creative Dashboard"""
    # Get filter parameter
    search_filter = request.args.get('filter', '').lower()
    
    creative_data = tool.get_creative_dashboard_data()
    
    # Filter data if search term provided
    if search_filter:
        creative_data = [
            ad for ad in creative_data 
            if search_filter in ad['ad_name'].lower()
        ]
    
    return jsonify(creative_data)

@app.route('/api/adgroup-dashboard-data')
def adgroup_dashboard_data():
    """Get ad set level grouped data for Ad Group Dashboard"""
    # Get filter parameter
    search_filter = request.args.get('filter', '').lower()
    
    adgroup_data = tool.get_adgroup_dashboard_data()
    
    # Filter data if search term provided
    if search_filter:
        adgroup_data = [
            adset for adset in adgroup_data 
            if search_filter in adset['ad_set_name'].lower() or
            any(search_filter in ad['ad_name'].lower() for ad in adset['ads'])
        ]
    
    return jsonify(adgroup_data)

@app.route('/api/optimization-recommendations')
def optimization_recommendations():
    recommendations = tool.get_optimization_recommendations()
    return jsonify(recommendations)

@app.route('/api/creative-data')
def creative_data():
    """Get Facebook creative data for AI insights"""
    return jsonify(tool.creative_data)

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

@app.route('/api/kpi-settings', methods=['GET', 'POST'])
def kpi_settings():
    if request.method == 'POST':
        try:
            new_settings = request.json
            tool.kpi_settings.update(new_settings)
            success = tool.save_kpi_settings()
            
            if success:
                # Reprocess data with new settings
                tool.process_combined_data()
                return jsonify({'status': 'success', 'message': 'KPI settings updated successfully'})
            else:
                return jsonify({'status': 'error', 'message': 'Failed to save KPI settings'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})
    else:
        return jsonify(tool.kpi_settings)

@app.route('/api/optimization-rules', methods=['GET', 'POST'])
def optimization_rules():
    if request.method == 'POST':
        try:
            new_rules = request.json
            tool.optimization_rules.update(new_rules)
            success = tool.save_optimization_rules()
            
            if success:
                return jsonify({'status': 'success', 'message': 'Optimization rules updated successfully'})
            else:
                return jsonify({'status': 'error', 'message': 'Failed to save optimization rules'})
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)})
    else:
        return jsonify(tool.optimization_rules)

if __name__ == '__main__':
    print("✅ Data loaded successfully")
    app.run(host='0.0.0.0', port=8080, debug=False)

