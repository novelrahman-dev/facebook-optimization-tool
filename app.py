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
        self.fb_creative_data = {'adsets': [], 'ads': [], 'last_updated': None}
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
            if not self.google_credentials:
                print("❌ Google credentials not found")
                return
            
            # Parse credentials from environment variable
            creds_dict = json.loads(self.google_credentials)
            
            # Set up credentials
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.readonly'
            ]
            
            credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            self.gc = gspread.authorize(credentials)
            
            print("✅ Google Sheets API initialized")
            
        except Exception as e:
            print(f"❌ Google Sheets initialization error: {e}")

    def init_facebook_api(self):
        """Initialize Facebook API for creative insights only"""
        try:
            if not self.fb_access_token or not self.fb_ad_account_id:
                print("⚠️ Facebook API credentials not available")
                return
            
            print("✅ Facebook API credentials configured")
            
        except Exception as e:
            print(f"❌ Facebook API initialization error: {e}")

    def load_data(self):
        """Load data from all sources"""
        try:
            self.load_google_sheets_data()
            self.process_combined_data()
            # Load Facebook creative data in background for AI insights
            self.load_facebook_creative_data()
            
        except Exception as e:
            print(f"❌ Data loading error: {e}")

    def load_google_sheets_data(self):
        """Load data from Google Sheets"""
        try:
            # Web Pages data
            web_sheet = self.gc.open("Opencare Facebook Ads Performance Tracker").worksheet("Web Pages")
            web_data = web_sheet.get_all_records()
            self.web_data = [row for row in web_data if row.get('Web Pages UTM Content') and str(row.get('Web Pages UTM Content')).lower() != 'total']
            print(f"✅ Loaded {len(self.web_data)} rows from web_pages")
            
            # Attribution data
            attr_sheet = self.gc.open("Opencare Facebook Ads Performance Tracker").worksheet("Attribution")
            attr_data = attr_sheet.get_all_records()
            self.attr_data = [row for row in attr_data if row.get('Attribution UTM Content') and str(row.get('Attribution UTM Content')).lower() != 'total']
            print(f"✅ Loaded {len(self.attr_data)} rows from attribution")
            
            # Facebook Spend data
            fb_sheet = self.gc.open("Opencare Facebook Ads Performance Tracker").worksheet("Facebook Spend")
            fb_data = fb_sheet.get_all_records()
            self.fb_data = [row for row in fb_data if row.get('Facebook Ad Name') and str(row.get('Facebook Ad Name')).lower() != 'total']
            print(f"✅ Loaded {len(self.fb_data)} rows from fb_spend")
            
        except Exception as e:
            print(f"❌ Google Sheets loading error: {e}")

    def load_facebook_creative_data(self):
        """Load Facebook creative data for AI insights only"""
        try:
            if not self.fb_access_token or not self.fb_ad_account_id:
                print("⚠️ Facebook API credentials not available for creative data")
                return
            
            print("🎨 Loading Facebook creative data for AI insights...")
            
            # Load ad sets with creative data
            adsets_url = f"https://graph.facebook.com/v18.0/{self.fb_ad_account_id}/adsets"
            adsets_params = {
                'access_token': self.fb_access_token,
                'fields': 'name,campaign{name},targeting,daily_budget,lifetime_budget,status',
                'limit': 1000
            }
            
            adsets_response = requests.get(adsets_url, params=adsets_params, timeout=30)
            adsets_data = []
            
            if adsets_response.status_code == 200:
                adsets_data = adsets_response.json().get('data', [])
                print(f"✅ Facebook creative data loaded: {len(adsets_data)} ad sets")
            else:
                print(f"⚠️ Facebook adsets API error: {adsets_response.status_code}")
            
            # Load ads with creative data
            ads_url = f"https://graph.facebook.com/v18.0/{self.fb_ad_account_id}/ads"
            ads_params = {
                'access_token': self.fb_access_token,
                'fields': 'name,adset{name},creative{title,body,image_url,video_id,object_story_spec},status',
                'limit': 1000
            }
            
            ads_response = requests.get(ads_url, params=ads_params, timeout=30)
            ads_data = []
            
            if ads_response.status_code == 200:
                ads_data = ads_response.json().get('data', [])
                print(f"✅ Facebook creative data loaded: {len(adsets_data)} ad sets, {len(ads_data)} ads")
            else:
                print(f"⚠️ Facebook ads API error: {ads_response.status_code}")
            
            # Store creative data for AI insights
            self.fb_creative_data = {
                'adsets': adsets_data,
                'ads': ads_data,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"❌ Facebook creative data loading error: {e}")

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
        """Process and combine data from Google Sheets only"""
        try:
            combined_data = []
            
            print(f"🔄 Processing Google Sheets data: {len(self.web_data)} web, {len(self.attr_data)} attr, {len(self.fb_data)} fb")
            
            # Create lookup dictionaries for Google Sheets data
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
            
            print(f"🔍 Created lookups: {len(web_lookup)} web, {len(attr_lookup)} attr")
            
            # Process FB spend data as primary source and combine with web/attribution data
            processed_count = 0
            for fb_row in self.fb_data:
                ad_set_name = str(fb_row.get('Facebook Adset Name', '')).strip()
                ad_name = str(fb_row.get('Facebook Ad Name', '')).strip()
                
                # Skip empty rows
                if not ad_set_name or not ad_name or ad_set_name.lower() == 'total' or ad_name.lower() == 'total':
                    continue
                
                # Try to find matching web and attribution data
                web_row = {}
                attr_row = {}
                
                # Try different key combinations for matching
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
                
                # Get spend and impressions from Facebook Google Sheets
                spend = self.safe_float(fb_row.get('Facebook Total Spend (USD)', 0))
                impressions = self.safe_float(fb_row.get('Facebook Total Impressions', 0))
                
                # Calculate clicks as Web Pages Unique Count × 0.9 (as requested)
                site_visits = self.safe_float(web_row.get('Web Pages Unique Count of Landing Pages', 0))
                clicks = site_visits * 0.9  # New calculation method
                
                # Calculate marketing metrics using Google Sheets data
                ctr = (clicks / impressions * 100) if impressions > 0 else 0
                cpc = spend / clicks if clicks > 0 else 0
                cpm = spend / impressions * 1000 if impressions > 0 else 0
                
                # Web data
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
        avg_completion_rate = sum(ad['completion_rate'] for ad in self.performance_data) / len(self.performance_data) * 100
        avg_funnel_start_rate = sum(ad['funnel_start_rate'] for ad in self.performance_data) / len(self.performance_data)
        avg_booking_rate = sum(ad['booking_conversion_rate'] for ad in self.performance_data) / len(self.performance_data)
        
        # Count successful ads
        successful_ads = sum(1 for ad in self.performance_data if ad['all_criteria_met'])
        
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
            'avg_cpa': avg_cpa,
            'avg_roas': avg_roas,
            'avg_cpm': avg_cpm,
            'avg_completion_rate': avg_completion_rate,
            'avg_funnel_start_rate': avg_funnel_start_rate,
            'avg_booking_rate': avg_booking_rate,
            'overall_roas': overall_roas,
            'overall_ctr': overall_ctr,
            'roi': roi,
            'successful_ads': successful_ads
        }

    def get_creative_dashboard_data(self, search_filter=''):
        """Get creative dashboard data (ad-level grouping)"""
        if not self.performance_data:
            return []
        
        # Group by ad name
        ad_groups = {}
        for ad in self.performance_data:
            ad_name = ad['ad_name']
            
            # Apply search filter
            if search_filter and search_filter.lower() not in ad_name.lower():
                continue
            
            if ad_name not in ad_groups:
                ad_groups[ad_name] = {
                    'ad_name': ad_name,
                    'ad_count': 0,
                    'spend': 0,
                    'clicks': 0,
                    'impressions': 0,
                    'bookings': 0,
                    'revenue': 0,
                    'promo_spend': 0,
                    'site_visits': 0,
                    'funnel_starts': 0,
                    'success_count': 0
                }
            
            group = ad_groups[ad_name]
            group['ad_count'] += 1
            group['spend'] += ad['spend']
            group['clicks'] += ad['clicks']
            group['impressions'] += ad['impressions']
            group['bookings'] += ad['bookings']
            group['revenue'] += ad['revenue']
            group['promo_spend'] += ad['promo_spend']
            group['site_visits'] += ad['site_visits']
            group['funnel_starts'] += ad['funnel_starts']
            
            # Count success criteria
            success_count = sum(1 for criteria in ad['success_criteria'].values() if criteria)
            group['success_count'] = max(group['success_count'], success_count)
        
        # Calculate metrics for each group
        result = []
        for group in ad_groups.values():
            ctr = (group['clicks'] / group['impressions'] * 100) if group['impressions'] > 0 else 0
            cpc = group['spend'] / group['clicks'] if group['clicks'] > 0 else 0
            cpm = group['spend'] / group['impressions'] * 1000 if group['impressions'] > 0 else 0
            cpa = group['spend'] / group['bookings'] if group['bookings'] > 0 else 0
            
            funnel_start_rate = (group['funnel_starts'] / group['site_visits'] * 100) if group['site_visits'] > 0 else 0
            booking_conversion_rate = (group['bookings'] / group['site_visits'] * 100) if group['site_visits'] > 0 else 0
            
            total_cost = group['spend'] + group['promo_spend']
            effective_bookings = group['bookings'] * 0.45  # Default completion rate
            cac = total_cost / effective_bookings if effective_bookings > 0 else 0
            ltv = group['revenue'] / effective_bookings if effective_bookings > 0 else 0
            roas = ltv / cac if cac > 0 else 0
            
            result.append({
                'ad_name': group['ad_name'],
                'ad_count': group['ad_count'],
                'spend': group['spend'],
                'clicks': group['clicks'],
                'impressions': group['impressions'],
                'ctr': ctr,
                'cpc': cpc,
                'cpm': cpm,
                'funnel_start_rate': funnel_start_rate,
                'booking_conversion_rate': booking_conversion_rate,
                'completion_rate': 45.0,  # Default
                'bookings': group['bookings'],
                'cpa': cpa,
                'roas': roas,
                'success_count': group['success_count']
            })
        
        return sorted(result, key=lambda x: x['spend'], reverse=True)

    def get_adgroup_dashboard_data(self, search_filter=''):
        """Get ad group dashboard data (nested expandable view)"""
        if not self.performance_data:
            return []
        
        # Group by ad set name
        adset_groups = {}
        for ad in self.performance_data:
            ad_set_name = ad['ad_set_name']
            
            # Apply search filter
            if search_filter and search_filter.lower() not in ad_set_name.lower() and search_filter.lower() not in ad['ad_name'].lower():
                continue
            
            if ad_set_name not in adset_groups:
                adset_groups[ad_set_name] = {
                    'ad_set_name': ad_set_name,
                    'spend': 0,
                    'clicks': 0,
                    'impressions': 0,
                    'bookings': 0,
                    'revenue': 0,
                    'promo_spend': 0,
                    'site_visits': 0,
                    'funnel_starts': 0,
                    'ads': [],
                    'successful_ads': 0,
                    'total_ads': 0
                }
            
            group = adset_groups[ad_set_name]
            group['spend'] += ad['spend']
            group['clicks'] += ad['clicks']
            group['impressions'] += ad['impressions']
            group['bookings'] += ad['bookings']
            group['revenue'] += ad['revenue']
            group['promo_spend'] += ad['promo_spend']
            group['site_visits'] += ad['site_visits']
            group['funnel_starts'] += ad['funnel_starts']
            group['total_ads'] += 1
            
            if ad['all_criteria_met']:
                group['successful_ads'] += 1
            
            # Add individual ad data
            success_count = sum(1 for criteria in ad['success_criteria'].values() if criteria)
            group['ads'].append({
                'ad_name': ad['ad_name'],
                'spend': ad['spend'],
                'clicks': ad['clicks'],
                'impressions': ad['impressions'],
                'ctr': ad['ctr'],
                'cpc': ad['cpc'],
                'cpm': ad['cpm'],
                'funnel_start_rate': ad['funnel_start_rate'],
                'booking_conversion_rate': ad['booking_conversion_rate'],
                'completion_rate': ad['completion_rate'] * 100,
                'bookings': ad['bookings'],
                'cpa': ad['cpa'],
                'roas': ad['roas'],
                'success_count': success_count
            })
        
        # Calculate metrics for each group
        result = []
        for group in adset_groups.values():
            ctr = (group['clicks'] / group['impressions'] * 100) if group['impressions'] > 0 else 0
            cpc = group['spend'] / group['clicks'] if group['clicks'] > 0 else 0
            cpm = group['spend'] / group['impressions'] * 1000 if group['impressions'] > 0 else 0
            cpa = group['spend'] / group['bookings'] if group['bookings'] > 0 else 0
            
            funnel_start_rate = (group['funnel_starts'] / group['site_visits'] * 100) if group['site_visits'] > 0 else 0
            booking_conversion_rate = (group['bookings'] / group['site_visits'] * 100) if group['site_visits'] > 0 else 0
            
            total_cost = group['spend'] + group['promo_spend']
            effective_bookings = group['bookings'] * 0.45  # Default completion rate
            cac = total_cost / effective_bookings if effective_bookings > 0 else 0
            ltv = group['revenue'] / effective_bookings if effective_bookings > 0 else 0
            roas = ltv / cac if cac > 0 else 0
            
            result.append({
                'ad_set_name': group['ad_set_name'],
                'spend': group['spend'],
                'clicks': group['clicks'],
                'impressions': group['impressions'],
                'ctr': ctr,
                'cpc': cpc,
                'cpm': cpm,
                'funnel_start_rate': funnel_start_rate,
                'booking_conversion_rate': booking_conversion_rate,
                'completion_rate': 45.0,  # Default
                'bookings': group['bookings'],
                'cpa': cpa,
                'roas': roas,
                'success_ratio': f"{group['successful_ads']}/{group['total_ads']}",
                'ads': group['ads']
            })
        
        return sorted(result, key=lambda x: x['spend'], reverse=True)

    def get_optimization_recommendations(self):
        """Generate optimization recommendations based on performance data"""
        if not self.performance_data:
            return []
        
        recommendations = []
        
        for ad in self.performance_data:
            spend = ad['spend']
            roas = ad['roas']
            cpa = ad['cpa']
            ctr = ad['ctr']
            cpc = ad['cpc']
            bookings = ad['bookings']
            
            # Pause recommendations
            if spend > self.optimization_rules['pause_spend_threshold'] and roas < self.optimization_rules['pause_roas_threshold']:
                priority = 'High' if spend > self.optimization_rules['high_priority_spend_threshold'] else 'Medium'
                recommendations.append({
                    'action': 'pause',
                    'ad_set_name': ad['ad_set_name'],
                    'ad_name': ad['ad_name'],
                    'reasoning': f'Low ROAS ({roas:.2f}) with significant spend (${spend:.2f})',
                    'priority': priority,
                    'spend': spend,
                    'current_metrics': {
                        'roas': roas,
                        'cpa': cpa,
                        'ctr': ctr,
                        'cpc': cpc,
                        'bookings': bookings
                    }
                })
            
            elif spend > self.optimization_rules['pause_cpa_spend_threshold'] and cpa > self.optimization_rules['pause_cpa_threshold']:
                priority = 'High' if spend > self.optimization_rules['high_priority_spend_threshold'] else 'Medium'
                recommendations.append({
                    'action': 'pause',
                    'ad_set_name': ad['ad_set_name'],
                    'ad_name': ad['ad_name'],
                    'reasoning': f'High CPA (${cpa:.2f}) with spend (${spend:.2f})',
                    'priority': priority,
                    'spend': spend,
                    'current_metrics': {
                        'roas': roas,
                        'cpa': cpa,
                        'ctr': ctr,
                        'cpc': cpc,
                        'bookings': bookings
                    }
                })
            
            elif spend > self.optimization_rules['pause_ctr_spend_threshold'] and ctr < self.optimization_rules['pause_ctr_threshold']:
                priority = 'Medium' if spend > self.optimization_rules['medium_priority_spend_threshold'] else 'Low'
                recommendations.append({
                    'action': 'pause',
                    'ad_set_name': ad['ad_set_name'],
                    'ad_name': ad['ad_name'],
                    'reasoning': f'Low CTR ({ctr:.2f}%) with spend (${spend:.2f})',
                    'priority': priority,
                    'spend': spend,
                    'current_metrics': {
                        'roas': roas,
                        'cpa': cpa,
                        'ctr': ctr,
                        'cpc': cpc,
                        'bookings': bookings
                    }
                })
            
            # Scale recommendations
            elif (spend > self.optimization_rules['scale_min_spend_threshold'] and 
                  roas > self.optimization_rules['scale_roas_threshold'] and
                  ad['all_criteria_met']):
                priority = 'High' if spend > self.optimization_rules['high_priority_spend_threshold'] else 'Medium'
                recommendations.append({
                    'action': 'scale',
                    'ad_set_name': ad['ad_set_name'],
                    'ad_name': ad['ad_name'],
                    'reasoning': f'High ROAS ({roas:.2f}) with all success criteria met',
                    'priority': priority,
                    'spend': spend,
                    'current_metrics': {
                        'roas': roas,
                        'cpa': cpa,
                        'ctr': ctr,
                        'cpc': cpc,
                        'bookings': bookings
                    }
                })
        
        return recommendations

# Initialize the tool
tool = FacebookOptimizationTool()

# Routes
@app.route('/')
def dashboard():
    return render_template('enhanced_dashboard.html')

@app.route('/api/performance-summary')
def api_performance_summary():
    return jsonify(tool.get_performance_summary())

@app.route('/api/creative-dashboard-data')
def api_creative_dashboard_data():
    search_filter = request.args.get('filter', '')
    return jsonify(tool.get_creative_dashboard_data(search_filter))

@app.route('/api/adgroup-dashboard-data')
def api_adgroup_dashboard_data():
    search_filter = request.args.get('filter', '')
    return jsonify(tool.get_adgroup_dashboard_data(search_filter))

@app.route('/api/optimization-recommendations')
def api_optimization_recommendations():
    return jsonify(tool.get_optimization_recommendations())

@app.route('/api/creative-data')
def api_creative_data():
    return jsonify(tool.fb_creative_data)

@app.route('/api/kpi-settings', methods=['GET', 'POST'])
def api_kpi_settings():
    if request.method == 'POST':
        new_settings = request.json
        tool.kpi_settings.update(new_settings)
        success = tool.save_kpi_settings()
        if success:
            # Reprocess data with new settings
            tool.process_combined_data()
            return jsonify({'status': 'success', 'message': 'KPI settings updated'})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to save settings'})
    else:
        return jsonify(tool.kpi_settings)

@app.route('/api/optimization-rules', methods=['GET', 'POST'])
def api_optimization_rules():
    if request.method == 'POST':
        new_rules = request.json
        tool.optimization_rules.update(new_rules)
        success = tool.save_optimization_rules()
        if success:
            return jsonify({'status': 'success', 'message': 'Optimization rules updated'})
        else:
            return jsonify({'status': 'error', 'message': 'Failed to save rules'})
    else:
        return jsonify(tool.optimization_rules)

@app.route('/api/refresh-data')
def api_refresh_data():
    try:
        tool.load_data()
        return jsonify({'status': 'success', 'message': 'Data refreshed successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)

