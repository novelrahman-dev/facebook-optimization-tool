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
        
        # Data storage
        self.web_data = []
        self.attr_data = []
        self.fb_data = []
        self.performance_data = []
        self.creative_data = {}
        
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
            'pause_ctr_threshold': 0.20,
            'pause_no_bookings_threshold': 150.0,
            'pause_high_cpc_threshold': 15.0,
            
            # Scale rules
            'scale_roas_threshold': 2.0,
            'scale_min_spend_threshold': 50.0,
            'scale_ctr_bonus_threshold': 0.50,
            'scale_cpa_bonus_threshold': 80.0,
            'scale_booking_rate_threshold': 3.0,
            
            # Priority settings
            'high_priority_spend_threshold': 200.0,
            'medium_priority_spend_threshold': 100.0
        }
        
        # Initialize Google Sheets and start data loading
        self.initialize_google_sheets()
        self.load_google_sheets_data()
        self.process_combined_data()
        
        # Start background Facebook creative data loading
        self.start_background_loading()

    def initialize_google_sheets(self):
        """Initialize Google Sheets API connection"""
        try:
            if not self.google_credentials:
                print("⚠️ Google credentials not found")
                return
            
            # Parse credentials JSON
            creds_dict = json.loads(self.google_credentials)
            
            # Define the scope
            scope = [
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.readonly'
            ]
            
            # Create credentials
            credentials = Credentials.from_service_account_info(creds_dict, scopes=scope)
            
            # Initialize gspread client
            self.gc = gspread.authorize(credentials)
            
            print("✅ Google Sheets API initialized")
            
        except Exception as e:
            print(f"❌ Google Sheets initialization error: {e}")
            self.gc = None

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
            
            self.loading_status['google_sheets_loaded'] = True
            
        except Exception as e:
            print(f"❌ Google Sheets loading error: {e}")
            self.loading_status['error_message'] = str(e)

    def start_background_loading(self):
        """Start background loading of Facebook creative data"""
        def background_load():
            self.load_facebook_creative_data_background()
        
        thread = threading.Thread(target=background_load)
        thread.daemon = True
        thread.start()

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
            
            while page_count < 10:  # Limit to prevent timeout
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
            
            while page_count < 10:  # Limit to prevent timeout
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

    def safe_int(self, value, default=0):
        """Safely convert value to int"""
        try:
            if value is None or value == '' or value == 'None':
                return default
            # Handle string numbers with commas
            if isinstance(value, str):
                value = value.replace(',', '')
            return int(float(value))
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
                ad_set_name = str(row.get('Facebook Adset Name', '')).strip()
                ad_name = str(row.get('Facebook Ad Name', '')).strip()
                if ad_set_name and ad_name:
                    key = f"{ad_set_name}|||{ad_name}"
                    if key not in web_lookup:
                        web_lookup[key] = []
                    web_lookup[key].append(row)
            
            attr_lookup = {}
            for row in self.attr_data:
                ad_set_name = str(row.get('Facebook Adset Name', '')).strip()
                ad_name = str(row.get('Facebook Ad Name', '')).strip()
                if ad_set_name and ad_name:
                    key = f"{ad_set_name}|||{ad_name}"
                    if key not in attr_lookup:
                        attr_lookup[key] = []
                    attr_lookup[key].append(row)
            
            print(f"🔍 Created lookups: {len(web_lookup)} web, {len(attr_lookup)} attr")
            
            # Process FB spend data as primary source
            for fb_row in self.fb_data:
                try:
                    ad_set_name = str(fb_row.get('Facebook Adset Name', '')).strip()
                    ad_name = str(fb_row.get('Facebook Ad Name', '')).strip()
                    
                    # Skip totals and empty rows
                    if (not ad_set_name or not ad_name or 
                        ad_set_name.lower() == 'total' or ad_name.lower() == 'total'):
                        continue
                    
                    key = f"{ad_set_name}|||{ad_name}"
                    
                    # Get web data for this ad
                    web_rows = web_lookup.get(key, [])
                    web_unique_count = sum(self.safe_int(row.get('Unique Count of Landing Pages', 0)) for row in web_rows)
                    
                    # Get attribution data for this ad
                    attr_rows = attr_lookup.get(key, [])
                    total_revenue = sum(self.safe_float(row.get('Attibuted Revenue (USD)', 0)) for row in attr_rows)
                    total_bookings = sum(self.safe_float(row.get('Attibuted Bookings', 0)) for row in attr_rows)
                    total_offer_spend = sum(self.safe_float(row.get('Attibuted Offer Spend (Predicted) (USD)', 0)) for row in attr_rows)
                    
                    # Calculate Link Clicks = Web Pages Unique Count × 0.9
                    link_clicks = web_unique_count * 0.9
                    
                    # Get metrics from FB spend data
                    spend = self.safe_float(fb_row.get('Facebook Total Spend (USD)', 0))
                    impressions = self.safe_int(fb_row.get('Facebook Total Impressions', 0))
                    
                    # Calculate derived metrics
                    ctr = (link_clicks / impressions * 100) if impressions > 0 else 0
                    cpc = (spend / link_clicks) if link_clicks > 0 else 0
                    cpm = (spend / impressions * 1000) if impressions > 0 else 0
                    cpa = (spend / total_bookings) if total_bookings > 0 else 0
                    roas = (total_revenue / spend) if spend > 0 else 0
                    
                    # Calculate funnel metrics
                    funnel_start_rate = (web_unique_count / impressions * 100) if impressions > 0 else 0
                    booking_conversion_rate = (total_bookings / web_unique_count * 100) if web_unique_count > 0 else 0
                    completion_rate = 0.45  # Default 45% as specified
                    
                    # Success criteria evaluation
                    success_criteria = [
                        ctr > self.kpi_settings['ctr_threshold'],
                        funnel_start_rate > self.kpi_settings['funnel_start_threshold'],
                        cpa < self.kpi_settings['cpa_threshold'] and cpa > 0,
                        link_clicks > self.kpi_settings['clicks_threshold'],
                        roas > self.kpi_settings['roas_threshold'],
                        cpc < self.kpi_settings['cpc_threshold'] and cpc > 0,
                        cpm < self.kpi_settings['cpm_threshold'] and cpm > 0,
                        booking_conversion_rate > self.kpi_settings['booking_conversion_threshold']
                    ]
                    success_count = sum(success_criteria)
                    
                    combined_record = {
                        'ad_set_name': ad_set_name,
                        'ad_name': ad_name,
                        'spend': spend,
                        'impressions': impressions,
                        'clicks': link_clicks,
                        'ctr': ctr,
                        'cpc': cpc,
                        'cpm': cpm,
                        'revenue': total_revenue,
                        'bookings': total_bookings,
                        'offer_spend': total_offer_spend,
                        'cpa': cpa,
                        'roas': roas,
                        'funnel_start_rate': funnel_start_rate,
                        'booking_conversion_rate': booking_conversion_rate,
                        'completion_rate': completion_rate,
                        'web_unique_count': web_unique_count,
                        'success_count': success_count,
                        'success_criteria': success_criteria
                    }
                    
                    combined_data.append(combined_record)
                    
                except Exception as e:
                    print(f"⚠️ Error processing row: {e}")
                    continue
            
            self.performance_data = combined_data
            self.loading_status['data_processed'] = True
            self.loading_status['last_updated'] = datetime.now().isoformat()
            
            print(f"✅ Processed {len(combined_data)} combined records from Google Sheets data")
            
        except Exception as e:
            print(f"❌ Error processing combined data: {e}")
            self.loading_status['error_message'] = str(e)

    def get_performance_summary(self):
        """Generate performance summary metrics"""
        try:
            if not self.performance_data:
                return self.get_default_summary()
            
            total_ads = len(self.performance_data)
            total_spend = sum(ad['spend'] for ad in self.performance_data)
            total_revenue = sum(ad['revenue'] for ad in self.performance_data)
            total_impressions = sum(ad['impressions'] for ad in self.performance_data)
            total_clicks = sum(ad['clicks'] for ad in self.performance_data)
            total_bookings = sum(ad['bookings'] for ad in self.performance_data)
            total_offer_spend = sum(ad['offer_spend'] for ad in self.performance_data)
            
            # Calculate averages and totals
            overall_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
            avg_cpc = (total_spend / total_clicks) if total_clicks > 0 else 0
            avg_cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
            overall_roas = (total_revenue / total_spend) if total_spend > 0 else 0
            avg_cpa = (total_spend / total_bookings) if total_bookings > 0 else 0
            
            # Calculate average rates
            avg_completion_rate = sum(ad['completion_rate'] for ad in self.performance_data) / total_ads * 100
            avg_funnel_start_rate = sum(ad['funnel_start_rate'] for ad in self.performance_data) / total_ads
            avg_booking_rate = sum(ad['booking_conversion_rate'] for ad in self.performance_data) / total_ads
            
            # Calculate ROI
            roi = ((total_revenue - total_offer_spend - total_spend) / total_revenue * 100) if total_revenue > 0 else 0
            
            # Count successful ads
            successful_ads = sum(1 for ad in self.performance_data if ad['success_count'] >= 6)
            
            return {
                'total_ads': total_ads,
                'total_spend': total_spend,
                'total_revenue': total_revenue,
                'total_impressions': total_impressions,
                'total_clicks': total_clicks,
                'total_bookings': total_bookings,
                'total_offer_spend': total_offer_spend,
                'overall_ctr': overall_ctr,
                'avg_cpc': avg_cpc,
                'avg_cpm': avg_cpm,
                'overall_roas': overall_roas,
                'avg_cpa': avg_cpa,
                'avg_completion_rate': avg_completion_rate,
                'avg_funnel_start_rate': avg_funnel_start_rate,
                'avg_booking_rate': avg_booking_rate,
                'roi': roi,
                'successful_ads': successful_ads
            }
            
        except Exception as e:
            print(f"❌ Error generating performance summary: {e}")
            return self.get_default_summary()

    def get_default_summary(self):
        """Return default summary when no data available"""
        return {
            'total_ads': 0,
            'total_spend': 0,
            'total_revenue': 0,
            'total_impressions': 0,
            'total_clicks': 0,
            'total_bookings': 0,
            'total_offer_spend': 0,
            'overall_ctr': 0,
            'avg_cpc': 0,
            'avg_cpm': 0,
            'overall_roas': 0,
            'avg_cpa': 0,
            'avg_completion_rate': 0,
            'avg_funnel_start_rate': 0,
            'avg_booking_rate': 0,
            'roi': 0,
            'successful_ads': 0
        }

    def get_creative_dashboard_data(self):
        """Get ad-level grouped data for Creative Dashboard"""
        try:
            if not self.performance_data:
                return []
            
            # Group by ad name
            ad_groups = defaultdict(list)
            for ad in self.performance_data:
                ad_groups[ad['ad_name']].append(ad)
            
            creative_data = []
            for ad_name, ads in ad_groups.items():
                # Aggregate metrics for this ad name
                total_spend = sum(ad['spend'] for ad in ads)
                total_impressions = sum(ad['impressions'] for ad in ads)
                total_clicks = sum(ad['clicks'] for ad in ads)
                total_revenue = sum(ad['revenue'] for ad in ads)
                total_bookings = sum(ad['bookings'] for ad in ads)
                total_web_unique = sum(ad['web_unique_count'] for ad in ads)
                
                # Calculate aggregated metrics
                ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                cpc = (total_spend / total_clicks) if total_clicks > 0 else 0
                cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
                cpa = (total_spend / total_bookings) if total_bookings > 0 else 0
                roas = (total_revenue / total_spend) if total_spend > 0 else 0
                funnel_start_rate = (total_web_unique / total_impressions * 100) if total_impressions > 0 else 0
                booking_conversion_rate = (total_bookings / total_web_unique * 100) if total_web_unique > 0 else 0
                completion_rate = 0.45  # Default
                
                # Success criteria evaluation
                success_criteria = [
                    ctr > self.kpi_settings['ctr_threshold'],
                    funnel_start_rate > self.kpi_settings['funnel_start_threshold'],
                    cpa < self.kpi_settings['cpa_threshold'] and cpa > 0,
                    total_clicks > self.kpi_settings['clicks_threshold'],
                    roas > self.kpi_settings['roas_threshold'],
                    cpc < self.kpi_settings['cpc_threshold'] and cpc > 0,
                    cpm < self.kpi_settings['cpm_threshold'] and cpm > 0,
                    booking_conversion_rate > self.kpi_settings['booking_conversion_threshold']
                ]
                success_count = sum(success_criteria)
                
                creative_data.append({
                    'ad_name': ad_name,
                    'ad_count': len(ads),
                    'spend': total_spend,
                    'impressions': total_impressions,
                    'clicks': total_clicks,
                    'ctr': ctr,
                    'cpc': cpc,
                    'cpm': cpm,
                    'revenue': total_revenue,
                    'bookings': total_bookings,
                    'cpa': cpa,
                    'roas': roas,
                    'funnel_start_rate': funnel_start_rate,
                    'booking_conversion_rate': booking_conversion_rate,
                    'completion_rate': completion_rate,
                    'success_count': success_count
                })
            
            # Sort by spend descending
            creative_data.sort(key=lambda x: x['spend'], reverse=True)
            return creative_data
            
        except Exception as e:
            print(f"❌ Error generating creative dashboard data: {e}")
            return []

    def get_adgroup_dashboard_data(self):
        """Get ad set level grouped data for Ad Group Dashboard"""
        try:
            if not self.performance_data:
                return []
            
            # Group by ad set name
            adset_groups = defaultdict(list)
            for ad in self.performance_data:
                adset_groups[ad['ad_set_name']].append(ad)
            
            adgroup_data = []
            for ad_set_name, ads in adset_groups.items():
                # Aggregate metrics for this ad set
                total_spend = sum(ad['spend'] for ad in ads)
                total_impressions = sum(ad['impressions'] for ad in ads)
                total_clicks = sum(ad['clicks'] for ad in ads)
                total_revenue = sum(ad['revenue'] for ad in ads)
                total_bookings = sum(ad['bookings'] for ad in ads)
                total_web_unique = sum(ad['web_unique_count'] for ad in ads)
                
                # Calculate aggregated metrics
                ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
                cpc = (total_spend / total_clicks) if total_clicks > 0 else 0
                cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
                cpa = (total_spend / total_bookings) if total_bookings > 0 else 0
                roas = (total_revenue / total_spend) if total_spend > 0 else 0
                funnel_start_rate = (total_web_unique / total_impressions * 100) if total_impressions > 0 else 0
                booking_conversion_rate = (total_bookings / total_web_unique * 100) if total_web_unique > 0 else 0
                completion_rate = 0.45  # Default
                
                # Count successful ads in this ad set
                successful_ads = sum(1 for ad in ads if ad['success_count'] >= 6)
                total_ads = len(ads)
                success_ratio = f"{successful_ads}/{total_ads}"
                
                # Process individual ads for nested view
                processed_ads = []
                for ad in ads:
                    processed_ads.append({
                        'ad_name': ad['ad_name'],
                        'spend': ad['spend'],
                        'impressions': ad['impressions'],
                        'clicks': ad['clicks'],
                        'ctr': ad['ctr'],
                        'cpc': ad['cpc'],
                        'cpm': ad['cpm'],
                        'revenue': ad['revenue'],
                        'bookings': ad['bookings'],
                        'cpa': ad['cpa'],
                        'roas': ad['roas'],
                        'funnel_start_rate': ad['funnel_start_rate'],
                        'booking_conversion_rate': ad['booking_conversion_rate'],
                        'completion_rate': ad['completion_rate'],
                        'success_count': ad['success_count']
                    })
                
                # Sort ads by spend descending
                processed_ads.sort(key=lambda x: x['spend'], reverse=True)
                
                adgroup_data.append({
                    'ad_set_name': ad_set_name,
                    'spend': total_spend,
                    'impressions': total_impressions,
                    'clicks': total_clicks,
                    'ctr': ctr,
                    'cpc': cpc,
                    'cpm': cpm,
                    'revenue': total_revenue,
                    'bookings': total_bookings,
                    'cpa': cpa,
                    'roas': roas,
                    'funnel_start_rate': funnel_start_rate,
                    'booking_conversion_rate': booking_conversion_rate,
                    'completion_rate': completion_rate,
                    'successful_ads': successful_ads,
                    'total_ads': total_ads,
                    'success_ratio': success_ratio,
                    'ads': processed_ads
                })
            
            # Sort by spend descending
            adgroup_data.sort(key=lambda x: x['spend'], reverse=True)
            return adgroup_data
            
        except Exception as e:
            print(f"❌ Error generating adgroup dashboard data: {e}")
            return []

    def get_optimization_recommendations(self):
        """Generate optimization recommendations based on performance data"""
        try:
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
                booking_rate = ad['booking_conversion_rate']
                
                # Determine priority based on spend
                if spend >= self.optimization_rules['high_priority_spend_threshold']:
                    priority = 'High'
                elif spend >= self.optimization_rules['medium_priority_spend_threshold']:
                    priority = 'Medium'
                else:
                    priority = 'Low'
                
                # Pause recommendations
                if (roas < self.optimization_rules['pause_roas_threshold'] and 
                    spend > self.optimization_rules['pause_spend_threshold']):
                    recommendations.append({
                        'ad_set_name': ad['ad_set_name'],
                        'ad_name': ad['ad_name'],
                        'action': 'pause',
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
                
                elif (cpa > self.optimization_rules['pause_cpa_threshold'] and 
                      spend > self.optimization_rules['pause_spend_threshold']):
                    recommendations.append({
                        'ad_set_name': ad['ad_set_name'],
                        'ad_name': ad['ad_name'],
                        'action': 'pause',
                        'reasoning': f'High CPA (${cpa:.2f}) with significant spend (${spend:.2f})',
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
                
                elif (ctr < self.optimization_rules['pause_ctr_threshold'] and 
                      spend > self.optimization_rules['pause_spend_threshold']):
                    recommendations.append({
                        'ad_set_name': ad['ad_set_name'],
                        'ad_name': ad['ad_name'],
                        'action': 'pause',
                        'reasoning': f'Low CTR ({ctr:.2f}%) with significant spend (${spend:.2f})',
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
                
                elif (bookings == 0 and spend > self.optimization_rules['pause_no_bookings_threshold']):
                    recommendations.append({
                        'ad_set_name': ad['ad_set_name'],
                        'ad_name': ad['ad_name'],
                        'action': 'pause',
                        'reasoning': f'No bookings with significant spend (${spend:.2f})',
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
                
                elif (cpc > self.optimization_rules['pause_high_cpc_threshold'] and 
                      spend > self.optimization_rules['pause_spend_threshold']):
                    recommendations.append({
                        'ad_set_name': ad['ad_set_name'],
                        'ad_name': ad['ad_name'],
                        'action': 'pause',
                        'reasoning': f'High CPC (${cpc:.2f}) with significant spend (${spend:.2f})',
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
                elif (roas > self.optimization_rules['scale_roas_threshold'] and 
                      spend > self.optimization_rules['scale_min_spend_threshold'] and
                      ad['success_count'] >= 6):
                    recommendations.append({
                        'ad_set_name': ad['ad_set_name'],
                        'ad_name': ad['ad_name'],
                        'action': 'scale',
                        'reasoning': f'High ROAS ({roas:.2f}) with good performance ({ad["success_count"]}/8 criteria)',
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
                
                elif (ctr > self.optimization_rules['scale_ctr_bonus_threshold'] and 
                      spend > self.optimization_rules['scale_min_spend_threshold']):
                    recommendations.append({
                        'ad_set_name': ad['ad_set_name'],
                        'ad_name': ad['ad_name'],
                        'action': 'scale',
                        'reasoning': f'Exceptional CTR ({ctr:.2f}%) - high engagement potential',
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
                
                elif (cpa < self.optimization_rules['scale_cpa_bonus_threshold'] and 
                      bookings > 0 and spend > self.optimization_rules['scale_min_spend_threshold']):
                    recommendations.append({
                        'ad_set_name': ad['ad_set_name'],
                        'ad_name': ad['ad_name'],
                        'action': 'scale',
                        'reasoning': f'Low CPA (${cpa:.2f}) with bookings - efficient conversion',
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
                
                elif (booking_rate > self.optimization_rules['scale_booking_rate_threshold'] and 
                      spend > self.optimization_rules['scale_min_spend_threshold']):
                    recommendations.append({
                        'ad_set_name': ad['ad_set_name'],
                        'ad_name': ad['ad_name'],
                        'action': 'scale',
                        'reasoning': f'High booking rate ({booking_rate:.1f}%) - strong conversion potential',
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
            
            # Sort by priority and spend
            priority_order = {'High': 3, 'Medium': 2, 'Low': 1}
            recommendations.sort(key=lambda x: (priority_order[x['priority']], x['spend']), reverse=True)
            
            return recommendations
            
        except Exception as e:
            print(f"❌ Error generating optimization recommendations: {e}")
            return []

    def save_kpi_settings(self):
        """Save KPI settings (placeholder for persistence)"""
        try:
            # In a real implementation, this would save to a database or file
            print("✅ KPI settings saved")
            return True
        except Exception as e:
            print(f"❌ Error saving KPI settings: {e}")
            return False

    def save_optimization_rules(self):
        """Save optimization rules (placeholder for persistence)"""
        try:
            # In a real implementation, this would save to a database or file
            print("✅ Optimization rules saved")
            return True
        except Exception as e:
            print(f"❌ Error saving optimization rules: {e}")
            return False

    def refresh_data(self):
        """Refresh all data sources"""
        try:
            print("🔄 Starting data refresh...")
            
            # Reset loading status
            self.loading_status = {
                'google_sheets_loaded': False,
                'facebook_creative_loading': False,
                'facebook_creative_loaded': False,
                'data_processed': False,
                'error_message': None,
                'last_updated': None
            }
            
            # Reload Google Sheets data
            self.load_google_sheets_data()
            self.process_combined_data()
            
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
    print("✅ Facebook Optimization Tool initialized")
    app.run(host='0.0.0.0', port=8080, debug=False)

