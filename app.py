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

app = Flask(__name__)
CORS(app)

class FacebookOptimizationTool:
    def __init__(self):
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.fb_access_token = os.getenv('FB_ACCESS_TOKEN')
        self.fb_ad_account_id = os.getenv('FB_AD_ACCOUNT_ID')
        self.google_credentials = os.getenv('GOOGLE_CREDENTIALS_JSON')
        
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
        
        # Load data
        self.performance_data = []
        self.web_data = []
        self.attr_data = []
        self.fb_data = []
        self.fb_api_data = []
        self.load_data()
        
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

    def load_data(self):
        """Load data from all sources"""
        try:
            # Load Google Sheets data
            self.load_google_sheets_data()
            
            # Load Facebook API data with pagination
            self.load_facebook_api_data()
            
            # Process and combine data
            self.process_combined_data()
            
            print("✅ Data loaded successfully")
        except Exception as e:
            print(f"❌ Data loading error: {e}")

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

    def load_facebook_api_data(self):
        """Load marketing metrics from Facebook API with expanded date range"""
        try:
            if not self.fb_access_token or not self.fb_ad_account_id:
                print("⚠️ Facebook API credentials not available")
                return
            
            # Use expanded date range but with better pagination
            since_date = "2025-06-01"  # Back to June 1st as requested
            until_date = datetime.now().strftime("%Y-%m-%d")
            
            print(f"🔄 Loading Facebook API data from {since_date} to {until_date}")
            
            all_ads_data = []
            after_cursor = None
            page_count = 0
            max_pages = 20  # Increased to get more historical data
            
            while page_count < max_pages:
                try:
                    # Facebook API endpoint for ads with insights
                    url = f"https://graph.facebook.com/v18.0/{self.fb_ad_account_id}/ads"
                    
                    params = {
                        'access_token': self.fb_access_token,
                        'fields': 'name,adset{name},insights{impressions,clicks,spend}',  # Minimal fields to reduce load
                        'time_range': json.dumps({
                            'since': since_date,
                            'until': until_date
                        }),
                        'limit': 20,  # Smaller page size
                        'level': 'ad'
                    }
                    
                    if after_cursor:
                        params['after'] = after_cursor
                    
                    print(f"📡 Fetching page {page_count + 1} from Facebook API...")
                    
                    response = requests.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        data = response.json()
                        ads_data = data.get('data', [])
                        
                        if not ads_data:
                            print("📄 No more ads data available")
                            break
                        
                        all_ads_data.extend(ads_data)
                        print(f"✅ Loaded {len(ads_data)} ads from page {page_count + 1}")
                        
                        # Check for next page
                        paging = data.get('paging', {})
                        cursors = paging.get('cursors', {})
                        after_cursor = cursors.get('after')
                        
                        if not after_cursor:
                            print("📄 Reached end of data")
                            break
                        
                        page_count += 1
                        
                        # Add delay to respect rate limits
                        time.sleep(0.5)
                        
                    elif response.status_code == 400:
                        error_data = response.json()
                        error_message = error_data.get('error', {}).get('message', 'Unknown error')
                        print(f"❌ Facebook API error: {response.status_code} - {error_message}")
                        break
                    
                    else:
                        print(f"❌ Facebook API error: {response.status_code} - {response.text}")
                        break
                        
                except requests.exceptions.Timeout:
                    print("⏰ Facebook API request timeout, retrying...")
                    time.sleep(2)
                    continue
                except Exception as e:
                    print(f"❌ Facebook API request error: {e}")
                    break
            
            # Process the collected ads data
            fb_api_records = []
            for ad in all_ads_data:
                try:
                    ad_name = ad.get('name', '')
                    adset_info = ad.get('adset', {})
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
                            'ad_id': ad.get('id', ''),
                            'ad_name': ad_name,
                            'adset_name': adset_name,
                            'impressions': total_impressions,
                            'clicks': total_clicks,
                            'spend': total_spend,
                            'ctr': ctr,
                            'cpc': cpc,
                            'cpm': cpm
                        })
                        
                except Exception as e:
                    print(f"⚠️ Error processing ad data: {e}")
                    continue
            
            self.fb_api_data = fb_api_records
            print(f"✅ Processed {len(fb_api_records)} ads from Facebook API")
                
        except Exception as e:
            print(f"❌ Facebook API loading error: {e}")
            self.fb_api_data = []

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
        """Process and combine data from all sources"""
        try:
            combined_data = []
            
            print(f"🔄 Processing data: {len(self.web_data)} web, {len(self.attr_data)} attr, {len(self.fb_data)} fb sheets, {len(self.fb_api_data)} fb api")
            
            # Create lookup dictionaries
            web_lookup = {}
            for row in self.web_data:
                utm_content = str(row.get('Web Pages UTM Content', '')).strip()
                utm_term = str(row.get('Web Pages UTM Term', '')).strip()
                if utm_content and utm_term and utm_content.lower() != 'total' and utm_term.lower() != 'total':
                    keys = [
                        f"{utm_term}_{utm_content}",
                        f"{utm_content}_{utm_term}",
                        utm_content,
                        utm_term
                    ]
                    for key in keys:
                        web_lookup[key.lower()] = row
            
            attr_lookup = {}
            for row in self.attr_data:
                utm_content = str(row.get('Attribution UTM Content', '')).strip()
                utm_term = str(row.get('Attribution UTM Term', '')).strip()
                if utm_content and utm_term and utm_content.lower() != 'total' and utm_term.lower() != 'total':
                    keys = [
                        f"{utm_term}_{utm_content}",
                        f"{utm_content}_{utm_term}",
                        utm_content,
                        utm_term
                    ]
                    for key in keys:
                        attr_lookup[key.lower()] = row
            
            # Create FB spend lookup (Google Sheets)
            fb_spend_lookup = {}
            for row in self.fb_data:
                ad_set_name = str(row.get('Facebook Adset Name', '')).strip()
                ad_name = str(row.get('Facebook Ad Name', '')).strip()
                if ad_set_name and ad_name and ad_set_name.lower() != 'total' and ad_name.lower() != 'total':
                    keys = [
                        f"{ad_set_name}_{ad_name}",
                        f"{ad_name}_{ad_set_name}",
                        ad_name,
                        ad_set_name
                    ]
                    for key in keys:
                        fb_spend_lookup[key.lower()] = row
            
            # Create FB API lookup (for marketing metrics)
            fb_api_lookup = {}
            for row in self.fb_api_data:
                ad_set_name = str(row.get('adset_name', '')).strip()
                ad_name = str(row.get('ad_name', '')).strip()
                if ad_set_name and ad_name:
                    keys = [
                        f"{ad_set_name}_{ad_name}",
                        f"{ad_name}_{ad_set_name}",
                        ad_name,
                        ad_set_name
                    ]
                    for key in keys:
                        fb_api_lookup[key.lower()] = row
            
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
                
                # Try to find matching data with flexible matching
                web_row = {}
                attr_row = {}
                fb_spend_row = {}
                
                # Try different key combinations
                search_keys = [
                    f"{ad_set_name}_{ad_name}",
                    f"{ad_name}_{ad_set_name}",
                    ad_name,
                    ad_set_name
                ]
                
                for key in search_keys:
                    key_lower = key.lower()
                    if key_lower in web_lookup:
                        web_row = web_lookup[key_lower]
                        break
                
                for key in search_keys:
                    key_lower = key.lower()
                    if key_lower in attr_lookup:
                        attr_row = attr_lookup[key_lower]
                        break
                
                for key in search_keys:
                    key_lower = key.lower()
                    if key_lower in fb_spend_lookup:
                        fb_spend_row = fb_spend_lookup[key_lower]
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
                
                # Attribution data
                bookings = self.safe_float(attr_row.get('Attribution Attributed NPRs', 0))
                revenue = self.safe_float(attr_row.get('Attribution Attibuted Total Revenue (Predicted) (USD)', 0))
                completion_rate = self.safe_float(attr_row.get('Attribution Attibuted PAS (Predicted)', 0.45))
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

    def get_creative_dashboard_data(self):
        """Get ad-level grouped data for Creative Dashboard"""
        try:
            # Group by ad name only
            ad_groups = defaultdict(list)
            
            for ad in self.performance_data:
                ad_name = ad['ad_name']
                ad_groups[ad_name].append(ad)
            
            # Aggregate data for each ad name
            creative_data = []
            for ad_name, ads in ad_groups.items():
                # Aggregate metrics
                total_spend = sum(ad['spend'] for ad in ads)
                total_impressions = sum(ad['impressions'] for ad in ads)
                total_clicks = sum(ad['clicks'] for ad in ads)
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
                funnel_start_rate = (total_funnel_starts / total_site_visits * 100) if total_site_visits > 0 else 0
                booking_conversion_rate = (total_bookings / total_site_visits * 100) if total_site_visits > 0 else 0
                
                # Calculate ROAS
                total_cost = total_spend + total_promo_spend
                avg_completion_rate = sum(ad['completion_rate'] for ad in ads) / len(ads)
                effective_bookings = total_bookings * avg_completion_rate
                cac = total_cost / effective_bookings if effective_bookings > 0 else 0
                ltv = total_revenue / effective_bookings if effective_bookings > 0 else 0
                roas = ltv / cac if cac > 0 else 0
                
                # Success criteria using dynamic KPI settings
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
                    'bookings': total_bookings,
                    'cpa': cpa,
                    'roas': roas,
                    'completion_rate': avg_completion_rate,
                    'success_criteria': success_criteria,
                    'criteria_met_count': sum(1 for v in success_criteria.values() if v),
                    'all_criteria_met': all(success_criteria.values())
                })
            
            return sorted(creative_data, key=lambda x: x['spend'], reverse=True)
            
        except Exception as e:
            print(f"❌ Error creating creative dashboard data: {e}")
            return []

    def get_ad_group_dashboard_data(self):
        """Get ad set level grouped data with nested ads for Ad Group Dashboard"""
        try:
            # Group by ad set name
            adset_groups = defaultdict(list)
            
            for ad in self.performance_data:
                ad_set_name = ad['ad_set_name']
                adset_groups[ad_set_name].append(ad)
            
            # Create nested structure
            adgroup_data = []
            for ad_set_name, ads in adset_groups.items():
                # Calculate ad set level aggregates
                total_spend = sum(ad['spend'] for ad in ads)
                total_impressions = sum(ad['impressions'] for ad in ads)
                total_clicks = sum(ad['clicks'] for ad in ads)
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
                funnel_start_rate = (total_funnel_starts / total_site_visits * 100) if total_site_visits > 0 else 0
                booking_conversion_rate = (total_bookings / total_site_visits * 100) if total_site_visits > 0 else 0
                
                # Calculate ROAS
                total_cost = total_spend + total_promo_spend
                avg_completion_rate = sum(ad['completion_rate'] for ad in ads) / len(ads)
                effective_bookings = total_bookings * avg_completion_rate
                cac = total_cost / effective_bookings if effective_bookings > 0 else 0
                ltv = total_revenue / effective_bookings if effective_bookings > 0 else 0
                roas = ltv / cac if cac > 0 else 0
                
                # Count successful ads
                successful_ads = len([ad for ad in ads if ad['all_criteria_met']])
                total_ads = len(ads)
                
                adgroup_data.append({
                    'ad_set_name': ad_set_name,
                    'total_ads': total_ads,
                    'successful_ads': successful_ads,
                    'success_ratio': f"{successful_ads}/{total_ads}",
                    'spend': total_spend,
                    'clicks': total_clicks,
                    'impressions': total_impressions,
                    'ctr': ctr,
                    'cpc': cpc,
                    'cpm': cpm,
                    'funnel_start_rate': funnel_start_rate,
                    'booking_conversion_rate': booking_conversion_rate,
                    'bookings': total_bookings,
                    'cpa': cpa,
                    'roas': roas,
                    'completion_rate': avg_completion_rate,
                    'ads': sorted(ads, key=lambda x: x['spend'], reverse=True)  # Nested ads sorted by spend
                })
            
            return sorted(adgroup_data, key=lambda x: x['spend'], reverse=True)
            
        except Exception as e:
            print(f"❌ Error creating ad group dashboard data: {e}")
            return []

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

    def get_optimization_recommendations(self):
        """Get optimization recommendations using configurable rules"""
        recommendations = []
        
        for ad in self.performance_data:
            # Determine priority based on spend
            if ad['spend'] >= self.optimization_rules['high_priority_spend_threshold']:
                priority = 'high'
            elif ad['spend'] >= self.optimization_rules['medium_priority_spend_threshold']:
                priority = 'medium'
            else:
                priority = 'low'
            
            # PAUSE RULES
            
            # Rule 1: Low ROAS with significant spend
            if (ad['roas'] < self.optimization_rules['pause_roas_threshold'] and 
                ad['spend'] > self.optimization_rules['pause_spend_threshold']):
                recommendations.append({
                    'id': f"pause_{ad['ad_id']}_{len(recommendations)}",
                    'action': 'pause',
                    'type': 'ad',
                    'ad_id': ad['ad_id'],
                    'name': ad['ad_name'],
                    'ad_set': ad['ad_set_name'],
                    'reason': f'Low ROAS ({ad["roas"]:.2f}) with significant spend (${ad["spend"]:.2f})',
                    'spend': ad['spend'],
                    'roas': ad['roas'],
                    'cpa': ad['cpa'],
                    'ctr': ad['ctr'],
                    'bookings': ad['bookings'],
                    'priority': priority,
                    'rule': 'low_roas_high_spend'
                })
            
            # Rule 2: High CPA above threshold
            elif (ad['cpa'] > self.optimization_rules['pause_cpa_threshold'] and 
                  ad['spend'] > self.optimization_rules['pause_cpa_spend_threshold']):
                recommendations.append({
                    'id': f"pause_{ad['ad_id']}_{len(recommendations)}",
                    'action': 'pause',
                    'type': 'ad',
                    'ad_id': ad['ad_id'],
                    'name': ad['ad_name'],
                    'ad_set': ad['ad_set_name'],
                    'reason': f'High CPA (${ad["cpa"]:.2f}) above threshold (${self.optimization_rules["pause_cpa_threshold"]:.2f})',
                    'spend': ad['spend'],
                    'roas': ad['roas'],
                    'cpa': ad['cpa'],
                    'ctr': ad['ctr'],
                    'bookings': ad['bookings'],
                    'priority': priority,
                    'rule': 'high_cpa'
                })
            
            # SCALE RULES
            
            # Rule 1: High ROAS and meets criteria
            elif (ad['roas'] > self.optimization_rules['scale_roas_threshold'] and 
                  ad['spend'] > self.optimization_rules['scale_min_spend_threshold']):
                
                # Check if all criteria required
                meets_criteria = True
                if self.optimization_rules['scale_all_criteria_required']:
                    meets_criteria = ad['all_criteria_met']
                
                if meets_criteria:
                    recommendations.append({
                        'id': f"scale_{ad['ad_id']}_{len(recommendations)}",
                        'action': 'scale',
                        'type': 'ad',
                        'ad_id': ad['ad_id'],
                        'name': ad['ad_name'],
                        'ad_set': ad['ad_set_name'],
                        'reason': f'High ROAS ({ad["roas"]:.2f}) above threshold ({self.optimization_rules["scale_roas_threshold"]:.2f}) and meets criteria',
                        'spend': ad['spend'],
                        'roas': ad['roas'],
                        'cpa': ad['cpa'],
                        'ctr': ad['ctr'],
                        'bookings': ad['bookings'],
                        'priority': 'high',
                        'rule': 'high_roas_meets_criteria'
                    })
        
        # Sort by priority and ROAS
        priority_order = {'high': 3, 'medium': 2, 'low': 1}
        return sorted(recommendations, 
                     key=lambda x: (priority_order.get(x['priority'], 0), x['roas']), 
                     reverse=True)

    def execute_optimization_actions(self, selected_recommendations):
        """Execute selected optimization actions via Facebook API"""
        results = []
        
        if not self.fb_access_token:
            return [{'status': 'error', 'message': 'Facebook API access token not configured'}]
        
        for rec_id in selected_recommendations:
            try:
                # Find the recommendation
                recommendations = self.get_optimization_recommendations()
                recommendation = next((r for r in recommendations if r['id'] == rec_id), None)
                
                if not recommendation:
                    results.append({
                        'id': rec_id,
                        'status': 'error',
                        'message': 'Recommendation not found'
                    })
                    continue
                
                ad_id = recommendation['ad_id']
                action = recommendation['action']
                
                # Execute the action via Facebook API
                if action == 'pause':
                    # Pause the ad
                    url = f"https://graph.facebook.com/v18.0/{ad_id}"
                    params = {
                        'access_token': self.fb_access_token,
                        'status': 'PAUSED'
                    }
                    
                    response = requests.post(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        results.append({
                            'id': rec_id,
                            'status': 'success',
                            'message': f'Successfully paused ad: {recommendation["name"]}',
                            'action': 'pause',
                            'ad_name': recommendation['name']
                        })
                    else:
                        results.append({
                            'id': rec_id,
                            'status': 'error',
                            'message': f'Failed to pause ad: {response.text}',
                            'action': 'pause',
                            'ad_name': recommendation['name']
                        })
                
                elif action == 'scale':
                    # For scaling, we'll increase the budget by 20%
                    # First get the current adset budget
                    adset_url = f"https://graph.facebook.com/v18.0/{ad_id}"
                    adset_params = {
                        'access_token': self.fb_access_token,
                        'fields': 'adset{daily_budget,lifetime_budget}'
                    }
                    
                    adset_response = requests.get(adset_url, params=adset_params, timeout=30)
                    
                    if adset_response.status_code == 200:
                        adset_data = adset_response.json()
                        adset_info = adset_data.get('adset', {})
                        
                        # Scale the budget (increase by 20%)
                        daily_budget = adset_info.get('daily_budget')
                        lifetime_budget = adset_info.get('lifetime_budget')
                        
                        if daily_budget:
                            new_budget = int(float(daily_budget) * 1.2)
                            budget_type = 'daily_budget'
                        elif lifetime_budget:
                            new_budget = int(float(lifetime_budget) * 1.2)
                            budget_type = 'lifetime_budget'
                        else:
                            results.append({
                                'id': rec_id,
                                'status': 'error',
                                'message': f'No budget found for ad set',
                                'action': 'scale',
                                'ad_name': recommendation['name']
                            })
                            continue
                        
                        # Update the adset budget
                        adset_id = adset_info.get('id')
                        if adset_id:
                            update_url = f"https://graph.facebook.com/v18.0/{adset_id}"
                            update_params = {
                                'access_token': self.fb_access_token,
                                budget_type: new_budget
                            }
                            
                            update_response = requests.post(update_url, params=update_params, timeout=30)
                            
                            if update_response.status_code == 200:
                                results.append({
                                    'id': rec_id,
                                    'status': 'success',
                                    'message': f'Successfully scaled ad set budget by 20%: {recommendation["name"]}',
                                    'action': 'scale',
                                    'ad_name': recommendation['name']
                                })
                            else:
                                results.append({
                                    'id': rec_id,
                                    'status': 'error',
                                    'message': f'Failed to scale ad set budget: {update_response.text}',
                                    'action': 'scale',
                                    'ad_name': recommendation['name']
                                })
                        else:
                            results.append({
                                'id': rec_id,
                                'status': 'error',
                                'message': f'Ad set ID not found',
                                'action': 'scale',
                                'ad_name': recommendation['name']
                            })
                    else:
                        results.append({
                            'id': rec_id,
                            'status': 'error',
                            'message': f'Failed to get ad set info: {adset_response.text}',
                            'action': 'scale',
                            'ad_name': recommendation['name']
                        })
                
            except Exception as e:
                results.append({
                    'id': rec_id,
                    'status': 'error',
                    'message': f'Exception occurred: {str(e)}',
                    'action': recommendation.get('action', 'unknown'),
                    'ad_name': recommendation.get('name', 'unknown')
                })
        
        return results

    def generate_ai_insights(self, analysis_type):
        """Generate AI insights based on performance data"""
        try:
            if not self.openai_api_key:
                return "OpenAI API key not configured"
            
            # Prepare performance summary for AI
            summary = self.get_performance_summary()
            top_performers = sorted(self.performance_data, key=lambda x: x['roas'], reverse=True)[:5]
            worst_performers = sorted(self.performance_data, key=lambda x: x['roas'])[:5]
            
            performance_context = f"""
            Performance Summary (Facebook API + Google Sheets Integration):
            - Total Ads: {summary['total_ads']}
            - Total Spend: ${summary['total_spend']:,.2f}
            - Total Revenue: ${summary['total_revenue']:,.2f}
            - Total Impressions: {summary['total_impressions']:,.0f}
            - Total Clicks: {summary['total_clicks']:,.0f}
            - Overall CTR: {summary['overall_ctr']:.2f}%
            - Average CPC: ${summary['avg_cpc']:.2f}
            - Average CPM: ${summary['avg_cpm']:.2f}
            - Overall ROAS: {summary['overall_roas']:.2f}
            - Average CPA: ${summary['avg_cpa']:.2f}
            - Successful Ads: {summary['successful_ads']}
            
            Current KPI Thresholds:
            - CTR > {self.kpi_settings['ctr_threshold']:.2f}%
            - Funnel Start Rate > {self.kpi_settings['funnel_start_threshold']:.1f}%
            - CPA < ${self.kpi_settings['cpa_threshold']:.2f}
            - Clicks > {self.kpi_settings['clicks_threshold']:,.0f}
            - ROAS > {self.kpi_settings['roas_threshold']:.2f}
            
            Current Optimization Rules:
            - Pause ROAS < {self.optimization_rules['pause_roas_threshold']:.2f}
            - Pause CPA > ${self.optimization_rules['pause_cpa_threshold']:.2f}
            - Scale ROAS > {self.optimization_rules['scale_roas_threshold']:.2f}
            
            Top 5 Performers (by ROAS):
            {chr(10).join([f"- {ad['ad_name'][:50]}... ROAS: {ad['roas']:.2f}, CTR: {ad['ctr']:.2f}%, CPC: ${ad['cpc']:.2f}" for ad in top_performers])}
            
            Worst 5 Performers (by ROAS):
            {chr(10).join([f"- {ad['ad_name'][:50]}... ROAS: {ad['roas']:.2f}, CTR: {ad['ctr']:.2f}%, CPC: ${ad['cpc']:.2f}" for ad in worst_performers])}
            """
            
            prompts = {
                'cluster': f"""
                Analyze the Facebook advertising performance data and provide cluster analysis insights:
                
                {performance_context}
                
                Please provide:
                1. Performance cluster patterns (high/medium/low performers)
                2. Audience segment insights based on naming conventions
                3. Creative format performance patterns
                4. Budget allocation recommendations
                5. Scaling opportunities
                """,
                
                'creative_copy': f"""
                Analyze the creative copy performance patterns:
                
                {performance_context}
                
                Please provide:
                1. Top performing creative patterns
                2. Messaging themes that drive results
                3. Copy optimization recommendations
                4. A/B testing suggestions for creative elements
                """,
                
                'cro': f"""
                Analyze conversion rate optimization opportunities:
                
                {performance_context}
                
                Focus on funnel performance:
                - Average Funnel Start Rate: {summary['avg_funnel_start_rate']:.1f}%
                - Average Booking Rate: {summary['avg_booking_rate']:.1f}%
                - Average Completion Rate: {summary['avg_completion_rate']*100:.1f}%
                
                Please provide:
                1. Funnel bottleneck analysis
                2. Landing page optimization recommendations
                3. Conversion rate improvement strategies
                4. Technical CRO suggestions
                """,
                
                'strategy': f"""
                Provide strategic recommendations based on performance data:
                
                {performance_context}
                
                Please provide:
                1. Campaign strategy optimization
                2. Budget reallocation recommendations
                3. Audience targeting improvements
                4. Long-term scaling strategy
                5. Risk mitigation for underperforming segments
                """
            }
            
            prompt = prompts.get(analysis_type, prompts['cluster'])
            
            headers = {
                'Authorization': f'Bearer {self.openai_api_key}',
                'Content-Type': 'application/json'
            }
            
            data = {
                'model': 'gpt-4',
                'messages': [
                    {
                        'role': 'system',
                        'content': 'You are an expert Facebook media buyer and data analyst. Provide actionable insights based on real performance data.'
                    },
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                'max_tokens': 1500,
                'temperature': 0.7
            }
            
            response = requests.post(
                'https://api.openai.com/v1/chat/completions',
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                return result['choices'][0]['message']['content']
            else:
                return f"Error generating insights: {response.status_code} - {response.text}"
                
        except Exception as e:
            return f"Error generating AI insights: {str(e)}"

# Initialize the tool
tool = FacebookOptimizationTool()

@app.route('/')
def dashboard():
    return render_template('enhanced_dashboard.html')

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

@app.route('/api/creative-dashboard')
def creative_dashboard():
    # Get filter parameter
    search_filter = request.args.get('filter', '').lower()
    
    # Get creative dashboard data
    creative_data = tool.get_creative_dashboard_data()
    
    # Filter data if search term provided
    if search_filter:
        creative_data = [
            ad for ad in creative_data 
            if search_filter in ad['ad_name'].lower()
        ]
    
    return jsonify(creative_data)

@app.route('/api/ad-group-dashboard')
def ad_group_dashboard():
    # Get filter parameter
    search_filter = request.args.get('filter', '').lower()
    
    # Get ad group dashboard data
    adgroup_data = tool.get_ad_group_dashboard_data()
    
    # Filter data if search term provided
    if search_filter:
        filtered_data = []
        for adgroup in adgroup_data:
            # Check if ad set name matches
            if search_filter in adgroup['ad_set_name'].lower():
                filtered_data.append(adgroup)
            else:
                # Check if any nested ad matches
                matching_ads = [
                    ad for ad in adgroup['ads']
                    if search_filter in ad['ad_name'].lower() or search_filter in ad['ad_set_name'].lower()
                ]
                if matching_ads:
                    # Create a copy with only matching ads
                    filtered_adgroup = adgroup.copy()
                    filtered_adgroup['ads'] = matching_ads
                    filtered_data.append(filtered_adgroup)
        adgroup_data = filtered_data
    
    return jsonify(adgroup_data)

@app.route('/api/optimization-recommendations')
def optimization_recommendations():
    recommendations = tool.get_optimization_recommendations()
    return jsonify(recommendations)

@app.route('/api/execute-optimizations', methods=['POST'])
def execute_optimizations():
    try:
        data = request.json
        selected_recommendations = data.get('recommendations', [])
        
        if not selected_recommendations:
            return jsonify({
                'status': 'error',
                'message': 'No recommendations selected'
            })
        
        # Execute the optimizations
        results = tool.execute_optimization_actions(selected_recommendations)
        
        return jsonify({
            'status': 'success',
            'message': f'Executed {len(selected_recommendations)} optimization actions',
            'results': results
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error executing optimizations: {str(e)}'
        })

@app.route('/api/ai-insights')
def ai_insights():
    analysis_type = request.args.get('type', 'cluster')
    insights = tool.generate_ai_insights(analysis_type)
    return jsonify({'insights': insights})

@app.route('/api/creative-brief')
def creative_brief():
    campaign_name = request.args.get('name', 'New Campaign')
    campaign_type = request.args.get('type', 'Seasonal')
    messaging_pillar = request.args.get('messaging_pillar', 'Convenience')
    value_pillar = request.args.get('value_pillar', 'Convenience-Insurance')
    creative_format = request.args.get('format', 'static')
    
    # Generate brief based on winning patterns
    brief = f"""
# Creative Brief: {campaign_name}

## Campaign Overview
**Campaign Type:** {campaign_type}
**Creative Format:** {creative_format.title()}
**Messaging Pillar:** {messaging_pillar}
**Value Pillar:** {value_pillar}

## Performance Context
Based on analysis of {len(tool.performance_data)} ads with Facebook API integration:
- Overall CTR: {tool.get_performance_summary()['overall_ctr']:.2f}%
- Average CPC: ${tool.get_performance_summary()['avg_cpc']:.2f}
- Average CPM: ${tool.get_performance_summary()['avg_cpm']:.2f}
- Overall ROAS: {tool.get_performance_summary()['overall_roas']:.2f}
- Successful ads show consistent patterns in messaging and targeting

## Success Criteria (Current KPI Settings)
Target metrics based on your current settings:
- CTR > {tool.kpi_settings['ctr_threshold']:.2f}%
- Funnel Start Rate > {tool.kpi_settings['funnel_start_threshold']:.1f}%
- CPA < ${tool.kpi_settings['cpa_threshold']:.2f}
- Clicks > {tool.kpi_settings['clicks_threshold']:,.0f}
- ROAS > {tool.kpi_settings['roas_threshold']:.2f}

## Messaging Framework
**Primary Message:** {messaging_pillar} focused messaging
**Value Proposition:** {value_pillar}
**Call to Action:** Book your appointment today

## Visual Direction
**Format:** {creative_format.title()} creative
**Style:** Clean, professional, trust-building
**Brand Elements:** Incorporate partner brand elements

## Reference Links
- Brand Guidelines: https://docs.google.com/presentation/d/1zCTFwviKE_MiKF5lpjFChdju1wWS0bl9GsmJmmw91Pk/edit
- Social Figma Board: https://www.figma.com/design/K7w8jbHpE2Dv0ZNbeNxDyw/Social-Promotions-and-Affliates-Master
- Creative Repository 1: https://drive.google.com/drive/folders/1TZG22ZvkFYIsMXchOKUiaVP4vgpqtcrH
- Creative Repository 2: https://drive.google.com/drive/folders/1rPfI1h5FoU94xsrh0tTXasspMJNDrMsc

## Example Ads
[To be added by user]
"""
    
    return jsonify({'brief': brief})

@app.route('/api/kpi-settings', methods=['GET'])
def get_kpi_settings():
    """Get current KPI settings"""
    return jsonify(tool.kpi_settings)

@app.route('/api/kpi-settings', methods=['POST'])
def update_kpi_settings():
    """Update KPI settings"""
    try:
        new_settings = request.json
        
        # Validate settings
        required_keys = ['ctr_threshold', 'funnel_start_threshold', 'cpa_threshold', 'clicks_threshold', 
                        'roas_threshold', 'cpc_threshold', 'cpm_threshold', 'booking_conversion_threshold']
        
        for key in required_keys:
            if key in new_settings:
                tool.kpi_settings[key] = float(new_settings[key])
        
        # Save settings
        success = tool.save_kpi_settings()
        
        if success:
            # Reprocess data with new criteria
            tool.process_combined_data()
            
            return jsonify({
                'status': 'success',
                'message': 'KPI settings updated and data reprocessed',
                'settings': tool.kpi_settings
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to save KPI settings'
            })
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error updating KPI settings: {str(e)}'
        })

@app.route('/api/kpi-settings/reset', methods=['POST'])
def reset_kpi_settings():
    """Reset KPI settings to defaults"""
    try:
        tool.kpi_settings = {
            'ctr_threshold': 0.30,
            'funnel_start_threshold': 15.0,
            'cpa_threshold': 120.0,
            'clicks_threshold': 500,
            'roas_threshold': 1.0,
            'cpc_threshold': 10.0,
            'cpm_threshold': 50.0,
            'booking_conversion_threshold': 2.0
        }
        
        success = tool.save_kpi_settings()
        
        if success:
            # Reprocess data with default criteria
            tool.process_combined_data()
            
            return jsonify({
                'status': 'success',
                'message': 'KPI settings reset to defaults',
                'settings': tool.kpi_settings
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to save default KPI settings'
            })
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error resetting KPI settings: {str(e)}'
        })

@app.route('/api/optimization-rules', methods=['GET'])
def get_optimization_rules():
    """Get current optimization rules"""
    return jsonify(tool.optimization_rules)

@app.route('/api/optimization-rules', methods=['POST'])
def update_optimization_rules():
    """Update optimization rules"""
    try:
        new_rules = request.json
        
        # Validate and update rules
        for key, value in new_rules.items():
            if key in tool.optimization_rules:
                if key == 'scale_all_criteria_required':
                    tool.optimization_rules[key] = bool(value)
                else:
                    tool.optimization_rules[key] = float(value)
        
        # Save rules
        success = tool.save_optimization_rules()
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Optimization rules updated successfully',
                'rules': tool.optimization_rules
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to save optimization rules'
            })
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error updating optimization rules: {str(e)}'
        })

@app.route('/api/optimization-rules/reset', methods=['POST'])
def reset_optimization_rules():
    """Reset optimization rules to defaults"""
    try:
        tool.optimization_rules = {
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
        
        success = tool.save_optimization_rules()
        
        if success:
            return jsonify({
                'status': 'success',
                'message': 'Optimization rules reset to defaults',
                'rules': tool.optimization_rules
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to save default optimization rules'
            })
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error resetting optimization rules: {str(e)}'
        })

@app.route('/api/refresh-data')
def refresh_data():
    try:
        tool.load_data()
        return jsonify({'status': 'success', 'message': 'Data refreshed successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/test-openai')
def test_openai():
    try:
        if not tool.openai_api_key:
            return jsonify({'status': 'error', 'message': 'OpenAI API key not configured'})
        
        headers = {
            'Authorization': f'Bearer {tool.openai_api_key}',
            'Content-Type': 'application/json'
        }
        
        data = {
            'model': 'gpt-3.5-turbo',
            'messages': [{'role': 'user', 'content': 'Hello, this is a test.'}],
            'max_tokens': 50
        }
        
        response = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers=headers,
            json=data,
            timeout=10
        )
        
        if response.status_code == 200:
            return jsonify({'status': 'success', 'message': 'OpenAI API working correctly'})
        else:
            return jsonify({'status': 'error', 'message': f'OpenAI API error: {response.status_code}'})
            
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/test-facebook')
def test_facebook():
    try:
        if not tool.fb_access_token or not tool.fb_ad_account_id:
            return jsonify({'status': 'error', 'message': 'Facebook API credentials not configured'})
        
        url = f"https://graph.facebook.com/v18.0/{tool.fb_ad_account_id}/ads"
        params = {
            'access_token': tool.fb_access_token,
            'fields': 'name,status',
            'limit': 5
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return jsonify({
                'status': 'success', 
                'message': f'Facebook API working - found {len(data.get("data", []))} ads'
            })
        else:
            return jsonify({
                'status': 'error', 
                'message': f'Facebook API error: {response.status_code} - {response.text}'
            })
            
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    print("✅ OpenAI API key configured" if tool.openai_api_key else "❌ OpenAI API key missing")
    print("✅ Facebook API credentials configured" if tool.fb_access_token and tool.fb_ad_account_id else "❌ Facebook API credentials missing")
    print(f"✅ KPI Settings loaded: {tool.kpi_settings}")
    print(f"✅ Optimization Rules loaded: {tool.optimization_rules}")
    app.run(host='0.0.0.0', port=8080, debug=False)

