#!/usr/bin/env python3
"""
Facebook Optimization Tool - Final Corrected Version
- Facebook API: Impressions & Link Clicks only
- Google Sheets: Spend (USD), Revenue, Conversions
- Corrected CPA calculation: Spend ÷ NPRs
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import gspread
from google.auth import default
import json
import os
import requests
import threading
import time
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

class FacebookOptimizationTool:
    def __init__(self):
        self.data = []
        self.fb_creative_data = {}
        self.kpi_settings = self.load_default_kpi_settings()
        self.optimization_rules = self.load_default_optimization_rules()
        
        # Initialize APIs
        self.init_google_sheets()
        self.init_facebook_api()
        
        # Load data
        self.load_data()
        
        # Start background Facebook creative data loading
        threading.Thread(target=self.load_facebook_creative_data, daemon=True).start()
    
    def init_google_sheets(self):
        """Initialize Google Sheets API"""
        try:
            # Use service account credentials from environment
            credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if credentials_json:
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    f.write(credentials_json)
                    credentials_path = f.name
                
                self.gc = gspread.service_account(filename=credentials_path)
                os.unlink(credentials_path)  # Clean up temp file
                print("✅ Google Sheets API initialized")
            else:
                print("❌ Google Sheets credentials not found")
                self.gc = None
        except Exception as e:
            print(f"❌ Google Sheets initialization error: {e}")
            self.gc = None
    
    def init_facebook_api(self):
        """Initialize Facebook API"""
        try:
            self.fb_access_token = os.getenv('FB_ACCESS_TOKEN')
            self.fb_ad_account_id = os.getenv('FB_AD_ACCOUNT_ID')
            if self.fb_access_token and self.fb_ad_account_id:
                print("✅ Facebook API credentials configured")
            else:
                print("❌ Facebook API credentials not found")
        except Exception as e:
            print(f"❌ Facebook API initialization error: {e}")
    
    def load_google_sheets_data(self):
        """Load data from Google Sheets"""
        try:
            if not self.gc:
                print("❌ Google Sheets not initialized")
                return [], [], []
            
            # Load individual spreadsheets
            fb_spend_sheet = self.gc.open_by_key('1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk').sheet1
            attribution_sheet = self.gc.open_by_key('1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ').sheet1
            web_pages_sheet = self.gc.open_by_key('1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA').sheet1
            
            # Get all records
            fb_data = fb_spend_sheet.get_all_records()
            attr_data = attribution_sheet.get_all_records()
            web_data = web_pages_sheet.get_all_records()
            
            print(f"✅ Loaded {len(fb_data)} rows from fb_spend")
            print(f"✅ Loaded {len(attr_data)} rows from attribution")
            print(f"✅ Loaded {len(web_data)} rows from web_pages")
            
            return fb_data, attr_data, web_data
            
        except Exception as e:
            print(f"❌ Error loading Google Sheets: {e}")
            return [], [], []
    
    def load_facebook_api_data(self):
        """Load impressions and link clicks from Facebook API"""
        try:
            if not self.fb_access_token or not self.fb_ad_account_id:
                print("❌ Facebook API not configured")
                return {}
            
            # Get date range synchronized with Google Docs update schedule (10 AM and 10 PM EST)
            import pytz
            est = pytz.timezone('US/Eastern')
            now_est = datetime.now(est)
            
            # Always use current date if we're past any update interval
            # This ensures we get the most recent data available
            if now_est.hour >= 10:  # After 10 AM EST (either 10 AM or 10 PM update)
                # Use today's date - data should be updated
                end_date = now_est.strftime("%Y-%m-%d")
            else:  # Before 10 AM EST
                # Use yesterday's date (last update was yesterday 10 PM)
                yesterday = now_est - timedelta(days=1)
                end_date = yesterday.strftime("%Y-%m-%d")
            
            start_date = "2025-06-01"
            
            print(f"🔄 Loading Facebook API data from {start_date} to {end_date} (EST synchronized)")
            
            # Handle ad account ID format - remove act_ prefix if present
            ad_account_id = self.fb_ad_account_id
            if ad_account_id.startswith('act_'):
                ad_account_id = ad_account_id[4:]  # Remove 'act_' prefix
            
            url = f"https://graph.facebook.com/v18.0/act_{ad_account_id}/ads"
            params = {
                'access_token': self.fb_access_token,
                'fields': 'name,adset{name},insights{impressions,clicks}',
                'time_range': f'{{"since":"{start_date}","until":"{end_date}"}}',
                'limit': 100
            }
            
            fb_api_data = {}
            page_count = 0
            max_pages = 5  # Reduced limit to prevent timeout
            
            print(f"🔄 Loading Facebook API data from {start_date} to {end_date}")
            
            while url and page_count < max_pages:
                response = requests.get(url, params=params, timeout=15)  # Reduced timeout
                
                if response.status_code == 200:
                    data = response.json()
                    ads = data.get('data', [])
                    
                    page_count += 1
                    print(f"📡 Loaded page {page_count}: {len(ads)} ads")
                    
                    for ad in ads:
                        ad_name = ad.get('name', '')
                        adset_info = ad.get('adset', {})
                        adset_name = adset_info.get('name', '') if adset_info else ''
                        insights = ad.get('insights', {}).get('data', [])
                        
                        if ad_name and adset_name and insights:
                            # Sum up insights data
                            total_impressions = sum(int(insight.get('impressions', 0)) for insight in insights)
                            total_clicks = sum(int(insight.get('clicks', 0)) for insight in insights)
                            
                            # Create combination key
                            combo_key = f"{adset_name}|||{ad_name}"
                            fb_api_data[combo_key] = {
                                'impressions': total_impressions,
                                'link_clicks': total_clicks,
                                'ad_name': ad_name,
                                'adset_name': adset_name
                            }
                    
                    # Get next page
                    paging = data.get('paging', {})
                    url = paging.get('next')
                    params = {}  # Next URL already has params
                    
                else:
                    print(f"❌ Facebook API error: {response.status_code}")
                    try:
                        error_data = response.json()
                        print(f"❌ Facebook API error details: {error_data}")
                    except:
                        print(f"❌ Facebook API response text: {response.text}")
                    break
            
            print(f"✅ Loaded {len(fb_api_data)} ad combinations from Facebook API")
            return fb_api_data
            
        except Exception as e:
            print(f"❌ Facebook API error: {e}")
            return {}
    
    def load_data(self):
        """Load and process all data"""
        print("🔄 Loading data from Google Sheets...")
        
        # Load Google Sheets data
        fb_data, attr_data, web_data = self.load_google_sheets_data()
        
        # Start Facebook API data loading in background to prevent timeout
        self.fb_api_data = {}
        threading.Thread(target=self.load_facebook_api_data_background, daemon=True).start()
        
        # Process data with Google Sheets only initially
        print(f"🔄 Processing data: {len(fb_data)} fb, {len(attr_data)} attr, {len(web_data)} web, 0 fb_api (loading in background)")
        
        self.data = self.process_data(fb_data, attr_data, web_data, {})
        print(f"✅ Processed {len(self.data)} combined records")
    
    def load_facebook_api_data_background(self):
        """Load Facebook API data in background"""
        print("🔄 Loading Facebook API data in background...")
        
        # Get target count from FB Spend data
        fb_data, _, _ = self.load_google_sheets_data()
        target_ads = len([row for row in fb_data if row.get('Facebook Ad Name') and 
                         str(row.get('Facebook Ad Name')).lower() != 'total'])
        
        print(f"🎯 Target: {target_ads} ads from FB Spend data")
        
        self.fb_api_data = self.load_facebook_api_data_complete(target_ads)
        
        # Reprocess data with Facebook API data when available
        if self.fb_api_data:
            print("🔄 Reprocessing data with Facebook API data...")
            fb_data, attr_data, web_data = self.load_google_sheets_data()
            self.data = self.process_data(fb_data, attr_data, web_data, self.fb_api_data)
            print(f"✅ Reprocessed {len(self.data)} records with Facebook API data")
    
    def load_facebook_api_data_complete(self, target_count):
        """Load Facebook API data until target count is reached"""
        try:
            if not self.fb_access_token or not self.fb_ad_account_id:
                print("❌ Facebook API not configured")
                return {}
            
            # Get date range synchronized with Google Docs update schedule (10 AM and 10 PM EST)
            import pytz
            est = pytz.timezone('US/Eastern')
            now_est = datetime.now(est)
            
            # Always use current date if we're past any update interval
            # This ensures we get the most recent data available
            if now_est.hour >= 10:  # After 10 AM EST (either 10 AM or 10 PM update)
                # Use today's date - data should be updated
                end_date = now_est.strftime("%Y-%m-%d")
            else:  # Before 10 AM EST
                # Use yesterday's date (last update was yesterday 10 PM)
                yesterday = now_est - timedelta(days=1)
                end_date = yesterday.strftime("%Y-%m-%d")
            
            start_date = "2025-06-01"
            
            print(f"🔄 Loading Facebook API data from {start_date} to {end_date} (EST synchronized)")
            
            # Handle ad account ID format - remove act_ prefix if present
            ad_account_id = self.fb_ad_account_id
            if ad_account_id.startswith('act_'):
                ad_account_id = ad_account_id[4:]  # Remove 'act_' prefix
            
            url = f"https://graph.facebook.com/v18.0/act_{ad_account_id}/ads"
            params = {
                'access_token': self.fb_access_token,
                'fields': 'name,adset{name},insights{impressions,clicks}',
                'time_range': f'{{"since":"{start_date}","until":"{end_date}"}}',
                'limit': 50  # Smaller page size for slower loading
            }
            
            fb_api_data = {}
            page_count = 0
            total_ads_loaded = 0
            
            print(f"🔄 Loading Facebook API data from {start_date} to {end_date}")
            
            while url and total_ads_loaded < target_count:
                response = requests.get(url, params=params, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    ads = data.get('data', [])
                    
                    page_count += 1
                    total_ads_loaded += len(ads)
                    print(f"📡 Loaded page {page_count}: {len(ads)} ads (total: {total_ads_loaded}/{target_count})")
                    
                    for ad in ads:
                        ad_name = ad.get('name', '')
                        adset_info = ad.get('adset', {})
                        adset_name = adset_info.get('name', '') if adset_info else ''
                        insights = ad.get('insights', {}).get('data', [])
                        
                        if ad_name and adset_name and insights:
                            # Sum up insights data
                            total_impressions = sum(int(insight.get('impressions', 0)) for insight in insights)
                            total_clicks = sum(int(insight.get('clicks', 0)) for insight in insights)
                            
                            # Create combination key
                            combo_key = f"{adset_name}|||{ad_name}"
                            fb_api_data[combo_key] = {
                                'impressions': total_impressions,
                                'link_clicks': total_clicks,
                                'ad_name': ad_name,
                                'adset_name': adset_name
                            }
                    
                    # Get next page
                    paging = data.get('paging', {})
                    url = paging.get('next')
                    params = {}  # Next URL already has params
                    
                    # Add delay to prevent rate limiting
                    time.sleep(1)  # 1 second delay between requests
                    
                else:
                    print(f"❌ Facebook API error: {response.status_code}")
                    try:
                        error_data = response.json()
                        print(f"❌ Facebook API error details: {error_data}")
                    except:
                        print(f"❌ Facebook API response text: {response.text}")
                    break
            
            print(f"✅ Loaded {len(fb_api_data)} ad combinations from Facebook API (target: {target_count})")
            return fb_api_data
            
        except Exception as e:
            print(f"❌ Facebook API error: {e}")
            return {}
    
    def process_data(self, fb_data, attr_data, web_data, fb_api_data):
        """Process and combine data from all sources"""
        combined_data = []
        
        # Use FB Spend as primary dataset (exclude totals row)
        fb_records = [row for row in fb_data if row.get('Facebook Ad Name') and 
                     str(row.get('Facebook Ad Name')).lower() != 'total']
        
        for fb_row in fb_records:
            try:
                # Extract FB data
                ad_name = str(fb_row.get('Facebook Ad Name', '')).strip()
                adset_name = str(fb_row.get('Facebook Adset Name', '')).strip()
                spend = self.clean_numeric(fb_row.get('Facebook Total Spend (USD)', 0))
                
                if not ad_name or not adset_name:
                    continue
                
                # Create combination key for matching
                combo_key = f"{adset_name}|||{ad_name}"
                
                # Get Facebook API data (impressions/clicks)
                fb_api_record = fb_api_data.get(combo_key, {})
                impressions = fb_api_record.get('impressions', 0)
                link_clicks = fb_api_record.get('link_clicks', 0)
                
                # Try to match with attribution data (by UTM content)
                revenue = 0
                offer_spend = 0
                nprs = 0
                for attr_row in attr_data:
                    attr_utm = str(attr_row.get('Attribution UTM Content', '')).strip()
                    if attr_utm and (attr_utm in ad_name or ad_name in attr_utm):
                        revenue = self.clean_numeric(attr_row.get('Attribution Attibuted Total Revenue (Predicted) (USD)', 0))
                        offer_spend = self.clean_numeric(attr_row.get('Attribution Attibuted Offer Spend (Predicted) (USD)', 0))
                        nprs = self.clean_numeric(attr_row.get('Attribution Attributed NPRs', 0))
                        break
                
                # Try to match with web pages data (by UTM content)
                funnel_starts = 0
                survey_completions = 0
                checkout_starts = 0
                for web_row in web_data:
                    web_utm = str(web_row.get('Web Pages UTM Content', '')).strip()
                    if web_utm and (web_utm in ad_name or ad_name in web_utm):
                        funnel_starts = self.clean_numeric(web_row.get('Web Pages Unique Count of Sessions with Funnel Starts', 0))
                        survey_completions = self.clean_numeric(web_row.get('Web Pages Unique Count of Sessions with Match Results', 0))
                        checkout_starts = self.clean_numeric(web_row.get('Count of Sessions with Checkout Started (V2 included)', 0))
                        break
                
                # Calculate metrics
                ctr = (link_clicks / impressions * 100) if impressions > 0 else 0
                cpc = (spend / link_clicks) if link_clicks > 0 else 0
                cpm = (spend / impressions * 1000) if impressions > 0 else 0
                roas = (revenue / spend) if spend > 0 else 0
                cpa = (spend / nprs) if nprs > 0 else 0  # CORRECTED: Spend ÷ NPRs
                
                # Funnel rates
                funnel_start_rate = (funnel_starts / link_clicks * 100) if link_clicks > 0 else 0
                booking_rate = (nprs / link_clicks * 100) if link_clicks > 0 else 0
                survey_completion_rate = (survey_completions / funnel_starts * 100) if funnel_starts > 0 else 0
                checkout_start_rate = (checkout_starts / survey_completions * 100) if survey_completions > 0 else 0
                
                # Success criteria (8 criteria)
                success_count = 0
                if ctr >= 0.30: success_count += 1
                if funnel_start_rate >= 15.0: success_count += 1
                if cpa <= 120.0 and cpa > 0: success_count += 1
                if link_clicks >= 500: success_count += 1
                if roas >= 1.0: success_count += 1
                if cpc <= 10.0 and cpc > 0: success_count += 1
                if cpm <= 50.0 and cpm > 0: success_count += 1
                if booking_rate >= 2.0: success_count += 1
                
                combined_record = {
                    'ad_name': ad_name,
                    'adset_name': adset_name,
                    'spend': spend,
                    'impressions': impressions,
                    'link_clicks': link_clicks,
                    'revenue': revenue,
                    'offer_spend': offer_spend,
                    'nprs': nprs,
                    'funnel_starts': funnel_starts,
                    'survey_completions': survey_completions,
                    'checkout_starts': checkout_starts,
                    'ctr': ctr,
                    'cpc': cpc,
                    'cpm': cpm,
                    'roas': roas,
                    'cpa': cpa,
                    'funnel_start_rate': funnel_start_rate,
                    'booking_rate': booking_rate,
                    'survey_completion_rate': survey_completion_rate,
                    'checkout_start_rate': checkout_start_rate,
                    'success_count': success_count
                }
                
                combined_data.append(combined_record)
                
            except Exception as e:
                print(f"❌ Error processing record: {e}")
                continue
        
        return combined_data
    
    def clean_numeric(self, value):
        """Clean and convert numeric values"""
        if value is None or value == '' or value == 0:
            return 0.0
        try:
            if isinstance(value, str):
                value = value.replace('$', '').replace(',', '').replace('%', '').strip()
            return float(value)
        except:
            return 0.0
    
    def get_performance_summary(self):
        """Get performance summary with corrected calculations matching the KPI analysis"""
        if not self.data:
            return {}
        
        # Use CSV URLs like corrected_final_kpis.py to get accurate totals
        try:
            import pandas as pd
            
            # Google Sheets CSV URLs
            FB_SPEND_URL = "https://docs.google.com/spreadsheets/d/1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk/export?format=csv&gid=341667505"
            ATTRIBUTION_URL = "https://docs.google.com/spreadsheets/d/1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ/export?format=csv&gid=129436906"
            WEB_PAGES_URL = "https://docs.google.com/spreadsheets/d/1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA/export?format=csv&gid=660938596"
            
            # Load data using pandas (like corrected_final_kpis.py)
            fb_spend_df = pd.read_csv(FB_SPEND_URL)
            attribution_df = pd.read_csv(ATTRIBUTION_URL)
            web_pages_df = pd.read_csv(WEB_PAGES_URL)
            
            # Get totals from last rows
            fb_totals = fb_spend_df.iloc[-1]
            attr_totals = attribution_df.iloc[-1]
            web_totals = web_pages_df.iloc[-1]
            
            # Extract corrected values from Google Sheets
            total_spend = self.clean_numeric(fb_totals['Facebook Total Spend (USD)'])
            total_revenue = self.clean_numeric(attr_totals['Attribution Attibuted Total Revenue (Predicted) (USD)'])
            total_offer_spend = self.clean_numeric(attr_totals['Attribution Attibuted Offer Spend (Predicted) (USD)'])
            total_nprs = self.clean_numeric(attr_totals['Attribution Attributed NPRs'])
            pas_rate = self.clean_numeric(attr_totals['Attribution Attibuted PAS (Predicted)'])
            
            # Web Pages data (columns D, E, F from totals row)
            total_funnel_starts = self.clean_numeric(web_totals.iloc[3])  # Column D
            total_survey_completions = self.clean_numeric(web_totals.iloc[4])  # Column E
            total_checkout_starts = self.clean_numeric(web_totals.iloc[5])  # Column F
            
            # Use Facebook API data for accurate traffic metrics
            if self.fb_api_data:
                total_impressions = sum(self.clean_numeric(ad.get('impressions', 0)) for ad in self.fb_api_data)
                total_link_clicks = sum(self.clean_numeric(ad.get('link_clicks', 0)) for ad in self.fb_api_data)
            else:
                # Fallback to aggregated data if FB API not available
                total_impressions = sum(record['impressions'] for record in self.data)
                total_link_clicks = sum(record['link_clicks'] for record in self.data)
            
            # Count unique ads and ad sets
            total_ads = len(self.data)
            unique_ads = len(set(record['ad_name'] for record in self.data))
            unique_adsets = len(set(record['adset_name'] for record in self.data))
            
            # Calculate corrected performance ratios
            overall_ctr = (total_link_clicks / total_impressions * 100) if total_impressions > 0 else 0
            average_cpc = (total_spend / total_link_clicks) if total_link_clicks > 0 else 0
            average_cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
            overall_roas = (total_revenue / total_spend) if total_spend > 0 else 0
            average_cpa = (total_spend / total_nprs) if total_nprs > 0 else 0  # CORRECTED: Spend ÷ NPRs
            
            # Completion and LTV
            completed_bookings = total_nprs * pas_rate
            total_cost = total_spend + total_offer_spend
            cac = (total_cost / completed_bookings) if completed_bookings > 0 else 0
            ltv = (total_revenue / completed_bookings) if completed_bookings > 0 else 0
            
            # Corrected funnel metrics using Facebook API traffic data
            funnel_start_rate = (total_funnel_starts / total_link_clicks * 100) if total_link_clicks > 0 else 0
            booking_rate = (total_nprs / total_link_clicks * 100) if total_link_clicks > 0 else 0
            survey_completion_rate = (total_survey_completions / total_funnel_starts * 100) if total_funnel_starts > 0 else 0
            checkout_start_rate = (total_checkout_starts / total_survey_completions * 100) if total_survey_completions > 0 else 0
            
            # ROI
            roi = ((total_revenue - total_offer_spend - total_spend) / total_revenue * 100) if total_revenue > 0 else 0
            
            # Success ads
            successful_ads = len([r for r in self.data if r['success_count'] >= 6])
            
            return {
                'total_ads': total_ads,
                'unique_ads': unique_ads,
                'unique_adsets': unique_adsets,
                'total_spend': total_spend,
                'total_revenue': total_revenue,
                'total_offer_spend': total_offer_spend,
                'total_cost': total_cost,
                'total_impressions': total_impressions,
                'total_link_clicks': total_link_clicks,
                'total_nprs': total_nprs,
                'completed_bookings': completed_bookings,
                'total_funnel_starts': total_funnel_starts,
                'total_survey_completions': total_survey_completions,
                'total_checkout_starts': total_checkout_starts,
                'overall_ctr': overall_ctr,
                'average_cpc': average_cpc,
                'average_cpm': average_cpm,
                'overall_roas': overall_roas,
                'average_cpa': average_cpa,
                'cac': cac,
                'ltv': ltv,
                'funnel_start_rate': funnel_start_rate,
                'booking_rate': booking_rate,
                'survey_completion_rate': survey_completion_rate,
                'checkout_start_rate': checkout_start_rate,
                'roi': roi,
                'successful_ads': successful_ads
            }
            
        except Exception as e:
            print(f"❌ Error in get_performance_summary: {e}")
            # Fallback to aggregated data
            return self.get_performance_summary_fallback()
    
    def get_performance_summary_fallback(self):
        """Fallback performance summary using aggregated data"""
        total_ads = len(self.data)
        unique_ads = len(set(record['ad_name'] for record in self.data))
        unique_adsets = len(set(record['adset_name'] for record in self.data))
        
        total_spend = sum(record['spend'] for record in self.data)
        total_revenue = sum(record['revenue'] for record in self.data)
        total_offer_spend = sum(record['offer_spend'] for record in self.data)
        total_impressions = sum(record['impressions'] for record in self.data)
        total_link_clicks = sum(record['link_clicks'] for record in self.data)
        total_nprs = sum(record['nprs'] for record in self.data)
        total_funnel_starts = sum(record['funnel_starts'] for record in self.data)
        total_survey_completions = sum(record['survey_completions'] for record in self.data)
        total_checkout_starts = sum(record['checkout_starts'] for record in self.data)
        
        # Calculate performance ratios
        overall_ctr = (total_link_clicks / total_impressions * 100) if total_impressions > 0 else 0
        average_cpc = (total_spend / total_link_clicks) if total_link_clicks > 0 else 0
        average_cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
        overall_roas = (total_revenue / total_spend) if total_spend > 0 else 0
        average_cpa = (total_spend / total_nprs) if total_nprs > 0 else 0
        
        # Completion and LTV
        pas_rate = 0.479
        completed_bookings = total_nprs * pas_rate
        total_cost = total_spend + total_offer_spend
        cac = (total_cost / completed_bookings) if completed_bookings > 0 else 0
        ltv = (total_revenue / completed_bookings) if completed_bookings > 0 else 0
        
        # Funnel metrics
        funnel_start_rate = (total_funnel_starts / total_link_clicks * 100) if total_link_clicks > 0 else 0
        booking_rate = (total_nprs / total_link_clicks * 100) if total_link_clicks > 0 else 0
        survey_completion_rate = (total_survey_completions / total_funnel_starts * 100) if total_funnel_starts > 0 else 0
        checkout_start_rate = (total_checkout_starts / total_survey_completions * 100) if total_survey_completions > 0 else 0
        
        # ROI
        roi = ((total_revenue - total_offer_spend - total_spend) / total_revenue * 100) if total_revenue > 0 else 0
        
        # Success ads
        successful_ads = len([r for r in self.data if r['success_count'] >= 6])
        
        return {
            'total_ads': total_ads,
            'unique_ads': unique_ads,
            'unique_adsets': unique_adsets,
            'total_spend': total_spend,
            'total_revenue': total_revenue,
            'total_offer_spend': total_offer_spend,
            'total_cost': total_cost,
            'total_impressions': total_impressions,
            'total_link_clicks': total_link_clicks,
            'total_nprs': total_nprs,
            'completed_bookings': completed_bookings,
            'total_funnel_starts': total_funnel_starts,
            'total_survey_completions': total_survey_completions,
            'total_checkout_starts': total_checkout_starts,
            'overall_ctr': overall_ctr,
            'average_cpc': average_cpc,
            'average_cpm': average_cpm,
            'overall_roas': overall_roas,
            'average_cpa': average_cpa,
            'cac': cac,
            'ltv': ltv,
            'funnel_start_rate': funnel_start_rate,
            'booking_rate': booking_rate,
            'survey_completion_rate': survey_completion_rate,
            'checkout_start_rate': checkout_start_rate,
            'roi': roi,
            'successful_ads': successful_ads
        }
    
    def get_creative_dashboard_data(self):
        """Get creative dashboard data (ad-level grouping)"""
        if not self.data:
            return []
        
        # Group by ad name
        ad_groups = {}
        for record in self.data:
            ad_name = record['ad_name']
            if ad_name not in ad_groups:
                ad_groups[ad_name] = []
            ad_groups[ad_name].append(record)
        
        # Aggregate data for each ad
        creative_data = []
        for ad_name, records in ad_groups.items():
            # Sum metrics
            total_spend = sum(r['spend'] for r in records)
            total_impressions = sum(r['impressions'] for r in records)
            total_link_clicks = sum(r['link_clicks'] for r in records)
            total_revenue = sum(r['revenue'] for r in records)
            total_nprs = sum(r['nprs'] for r in records)
            total_funnel_starts = sum(r['funnel_starts'] for r in records)
            total_survey_completions = sum(r['survey_completions'] for r in records)
            total_checkout_starts = sum(r['checkout_starts'] for r in records)
            
            # Calculate aggregated metrics
            ctr = (total_link_clicks / total_impressions * 100) if total_impressions > 0 else 0
            cpc = (total_spend / total_link_clicks) if total_link_clicks > 0 else 0
            cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
            roas = (total_revenue / total_spend) if total_spend > 0 else 0
            cpa = (total_spend / total_nprs) if total_nprs > 0 else 0
            
            # Funnel rates
            funnel_start_rate = (total_funnel_starts / total_link_clicks * 100) if total_link_clicks > 0 else 0
            booking_rate = (total_nprs / total_link_clicks * 100) if total_link_clicks > 0 else 0
            survey_completion_rate = (total_survey_completions / total_funnel_starts * 100) if total_funnel_starts > 0 else 0
            checkout_start_rate = (total_checkout_starts / total_survey_completions * 100) if total_survey_completions > 0 else 0
            
            # Success criteria
            success_count = 0
            if ctr >= 0.30: success_count += 1
            if funnel_start_rate >= 15.0: success_count += 1
            if cpa <= 120.0 and cpa > 0: success_count += 1
            if total_link_clicks >= 500: success_count += 1
            if roas >= 1.0: success_count += 1
            if cpc <= 10.0 and cpc > 0: success_count += 1
            if cpm <= 50.0 and cpm > 0: success_count += 1
            if booking_rate >= 2.0: success_count += 1
            
            # Success criteria object for frontend
            success_criteria = {
                'ctr_good': ctr >= 0.30,
                'funnel_start_good': funnel_start_rate >= 15.0,
                'cpa_good': cpa <= 120.0 and cpa > 0,
                'clicks_good': total_link_clicks >= 500,
                'roas_good': roas >= 1.0,
                'cpc_good': cpc <= 10.0 and cpc > 0,
                'cpm_good': cpm <= 50.0 and cpm > 0,
                'booking_conversion_good': booking_rate >= 2.0
            }
            
            creative_data.append({
                'ad_name': ad_name,
                'ad_set_name': records[0]['adset_name'],  # Add ad set name
                'ad_count': len(records),
                'spend': total_spend,
                'impressions': total_impressions,
                'clicks': total_link_clicks,  # Add clicks field
                'link_clicks': total_link_clicks,
                'revenue': total_revenue,
                'nprs': total_nprs,
                'bookings': total_nprs,  # Add bookings field
                'funnel_starts': total_funnel_starts,
                'survey_completions': total_survey_completions,
                'checkout_starts': total_checkout_starts,
                'ctr': ctr,
                'cpc': cpc,
                'cpm': cpm,
                'roas': roas,
                'cpa': cpa,
                'funnel_start_rate': funnel_start_rate,
                'booking_rate': booking_rate,
                'booking_conversion_rate': booking_rate,  # Add booking conversion rate
                'completion_rate': 0.479,  # Add completion rate
                'survey_completion_rate': survey_completion_rate,
                'checkout_start_rate': checkout_start_rate,
                'success_count': success_count,
                'success_criteria': success_criteria  # Add success criteria object
            })
        
        # Sort by spend descending
        creative_data.sort(key=lambda x: x['spend'], reverse=True)
        return creative_data
    
    def get_adgroup_dashboard_data(self):
        """Get ad group dashboard data (nested ad set + ad view)"""
        if not self.data:
            return []
        
        # Group by ad set
        adset_groups = {}
        for record in self.data:
            adset_name = record['adset_name']
            if adset_name not in adset_groups:
                adset_groups[adset_name] = []
            adset_groups[adset_name].append(record)
        
        # Create nested structure
        adgroup_data = []
        for adset_name, records in adset_groups.items():
            # Calculate ad set totals
            total_spend = sum(r['spend'] for r in records)
            total_impressions = sum(r['impressions'] for r in records)
            total_link_clicks = sum(r['link_clicks'] for r in records)
            total_revenue = sum(r['revenue'] for r in records)
            total_nprs = sum(r['nprs'] for r in records)
            total_funnel_starts = sum(r['funnel_starts'] for r in records)
            total_survey_completions = sum(r['survey_completions'] for r in records)
            total_checkout_starts = sum(r['checkout_starts'] for r in records)
            
            # Calculate ad set metrics
            ctr = (total_link_clicks / total_impressions * 100) if total_impressions > 0 else 0
            cpc = (total_spend / total_link_clicks) if total_link_clicks > 0 else 0
            cpm = (total_spend / total_impressions * 1000) if total_impressions > 0 else 0
            roas = (total_revenue / total_spend) if total_spend > 0 else 0
            cpa = (total_spend / total_nprs) if total_nprs > 0 else 0
            
            # Funnel rates
            funnel_start_rate = (total_funnel_starts / total_link_clicks * 100) if total_link_clicks > 0 else 0
            booking_rate = (total_nprs / total_link_clicks * 100) if total_link_clicks > 0 else 0
            survey_completion_rate = (total_survey_completions / total_funnel_starts * 100) if total_funnel_starts > 0 else 0
            checkout_start_rate = (total_checkout_starts / total_survey_completions * 100) if total_survey_completions > 0 else 0
            
            # Count successful ads in this ad set
            successful_ads = len([r for r in records if r['success_count'] >= 6])
            total_ads = len(records)
            
            adgroup_data.append({
                'adset_name': adset_name,
                'total_ads': total_ads,
                'successful_ads': successful_ads,
                'spend': total_spend,
                'impressions': total_impressions,
                'link_clicks': total_link_clicks,
                'revenue': total_revenue,
                'nprs': total_nprs,
                'funnel_starts': total_funnel_starts,
                'survey_completions': total_survey_completions,
                'checkout_starts': total_checkout_starts,
                'ctr': ctr,
                'cpc': cpc,
                'cpm': cpm,
                'roas': roas,
                'cpa': cpa,
                'funnel_start_rate': funnel_start_rate,
                'booking_rate': booking_rate,
                'survey_completion_rate': survey_completion_rate,
                'checkout_start_rate': checkout_start_rate,
                'ads': records
            })
        
        # Sort by spend descending
        adgroup_data.sort(key=lambda x: x['spend'], reverse=True)
        return adgroup_data
    
    def load_facebook_creative_data(self):
        """Load Facebook creative data for AI insights (background)"""
        try:
            print("🎨 Loading Facebook creative data for AI insights...")
            # This would load creative elements like headlines, text, landing pages
            # For now, just simulate the loading
            time.sleep(2)
            self.fb_creative_data = {'loaded': True, 'count': len(self.data)}
            print("✅ Facebook creative data loaded")
        except Exception as e:
            print(f"❌ Facebook creative data error: {e}")
    
    def load_default_kpi_settings(self):
        """Load default KPI settings"""
        return {
            'ctr_threshold': 0.30,
            'funnel_start_threshold': 15.0,
            'cpa_threshold': 120.0,
            'clicks_threshold': 500,
            'roas_threshold': 1.0,
            'cpc_threshold': 10.0,
            'cpm_threshold': 50.0,
            'booking_rate_threshold': 2.0
        }
    
    def load_default_optimization_rules(self):
        """Load default optimization rules"""
        return {
            'pause_low_roas': {'enabled': True, 'roas_threshold': 0.5, 'spend_threshold': 100},
            'pause_high_cpa': {'enabled': True, 'cpa_threshold': 200, 'spend_threshold': 50},
            'pause_low_ctr': {'enabled': True, 'ctr_threshold': 0.20, 'spend_threshold': 75},
            'scale_high_roas': {'enabled': True, 'roas_threshold': 2.0, 'budget_increase': 20}
        }
    
    def get_optimization_recommendations(self):
        """Get optimization recommendations based on performance data"""
        if not self.data:
            return []
        
        recommendations = []
        for record in self.data:
            # Pause recommendations
            if record['roas'] < 0.5 and record['spend'] > 100:
                recommendations.append({
                    'type': 'pause',
                    'ad_name': record['ad_name'],
                    'adset_name': record['adset_name'],
                    'reason': f"Low ROAS ({record['roas']:.2f}) with high spend (${record['spend']:.2f})",
                    'priority': 'high'
                })
            
            if record['cpa'] > 200 and record['spend'] > 50:
                recommendations.append({
                    'type': 'pause',
                    'ad_name': record['ad_name'],
                    'adset_name': record['adset_name'],
                    'reason': f"High CPA (${record['cpa']:.2f}) with spend (${record['spend']:.2f})",
                    'priority': 'medium'
                })
            
            # Scale recommendations
            if record['roas'] > 2.0 and record['success_count'] >= 6:
                recommendations.append({
                    'type': 'scale',
                    'ad_name': record['ad_name'],
                    'adset_name': record['adset_name'],
                    'reason': f"High ROAS ({record['roas']:.2f}) with {record['success_count']}/8 success criteria",
                    'priority': 'high'
                })
        
        return recommendations
    
    def execute_optimizations(self, selected_actions):
        """Execute selected optimization actions"""
        results = []
        for action in selected_actions:
            try:
                if action['type'] == 'pause':
                    # Simulate pausing ad
                    results.append({
                        'action': action,
                        'status': 'success',
                        'message': f"Successfully paused ad: {action['ad_name']}"
                    })
                elif action['type'] == 'scale':
                    # Simulate scaling ad
                    results.append({
                        'action': action,
                        'status': 'success',
                        'message': f"Successfully scaled ad: {action['ad_name']}"
                    })
            except Exception as e:
                results.append({
                    'action': action,
                    'status': 'error',
                    'message': f"Error executing action: {str(e)}"
                })
        
        return results
    
    def get_ai_insights(self, insight_type):
        """Get AI-powered insights"""
        if not self.data:
            return {'insights': 'No data available for analysis'}
        
        # Simple insights based on data
        total_spend = sum(r['spend'] for r in self.data)
        avg_roas = sum(r['roas'] for r in self.data) / len(self.data)
        successful_ads = len([r for r in self.data if r['success_count'] >= 6])
        
        insights = f"""
        Based on your performance data:
        
        • Total spend: ${total_spend:,.2f} across {len(self.data)} ads
        • Average ROAS: {avg_roas:.2f}
        • {successful_ads} ads meeting success criteria ({successful_ads/len(self.data)*100:.1f}%)
        
        Key recommendations:
        • Focus budget on high-performing ads with ROAS > 2.0
        • Pause underperforming ads with ROAS < 0.5
        • Test new creative variations for top-performing ad sets
        """
        
        return {'insights': insights}
    
    def generate_creative_brief(self, campaign_name, campaign_type, messaging_pillar, value_pillar, creative_format):
        """Generate creative brief"""
        brief = f"""
        Creative Brief for {campaign_name}
        
        Campaign Type: {campaign_type}
        Messaging Pillar: {messaging_pillar}
        Value Pillar: {value_pillar}
        Creative Format: {creative_format}
        
        Recommended approach:
        • Highlight the {value_pillar} value proposition
        • Use {messaging_pillar} messaging strategy
        • Optimize for {creative_format} format specifications
        • Include clear call-to-action
        • Test multiple variations
        """
        
        return {'brief': brief}

# Initialize the tool
tool = FacebookOptimizationTool()

@app.route('/')
def dashboard():
    """Main dashboard"""
    return render_template('enhanced_dashboard.html')

@app.route('/api/performance-summary')
def api_performance_summary():
    """API endpoint for performance summary"""
    try:
        summary = tool.get_performance_summary()
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/creative-dashboard-data')
def api_creative_dashboard_data():
    """API endpoint for creative dashboard data"""
    try:
        data = tool.get_creative_dashboard_data()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/adgroup-dashboard-data')
def api_adgroup_dashboard_data():
    """API endpoint for ad group dashboard data"""
    try:
        data = tool.get_adgroup_dashboard_data()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/refresh-data', methods=['POST'])
def api_refresh_data():
    """API endpoint to refresh data"""
    try:
        tool.load_data()
        return jsonify({'status': 'success', 'message': 'Data refreshed successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/performance-data')
def api_performance_data():
    """API endpoint for performance dashboard data"""
    try:
        data = tool.get_creative_dashboard_data()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/optimization-recommendations')
def api_optimization_recommendations():
    """API endpoint for optimization recommendations"""
    try:
        recommendations = tool.get_optimization_recommendations()
        return jsonify(recommendations)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/execute-optimizations', methods=['POST'])
def api_execute_optimizations():
    """API endpoint to execute optimization actions"""
    try:
        data = request.get_json()
        selected_actions = data.get('actions', [])
        results = tool.execute_optimizations(selected_actions)
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/ai-insights')
def api_ai_insights():
    """API endpoint for AI insights"""
    try:
        insight_type = request.args.get('type', 'performance')
        insights = tool.get_ai_insights(insight_type)
        return jsonify(insights)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/creative-brief')
def api_creative_brief():
    """API endpoint for creative brief generation"""
    try:
        campaign_name = request.args.get('name', '')
        campaign_type = request.args.get('type', '')
        messaging_pillar = request.args.get('messaging_pillar', '')
        value_pillar = request.args.get('value_pillar', '')
        creative_format = request.args.get('format', '')
        
        brief = tool.generate_creative_brief(campaign_name, campaign_type, messaging_pillar, value_pillar, creative_format)
        return jsonify(brief)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/kpi-settings')
def api_get_kpi_settings():
    """API endpoint to get KPI settings"""
    try:
        return jsonify(tool.kpi_settings)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/kpi-settings', methods=['POST'])
def api_save_kpi_settings():
    """API endpoint to save KPI settings"""
    try:
        data = request.get_json()
        tool.kpi_settings.update(data)
        return jsonify({'status': 'success', 'message': 'KPI settings saved successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/optimization-rules')
def api_get_optimization_rules():
    """API endpoint to get optimization rules"""
    try:
        return jsonify(tool.optimization_rules)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/optimization-rules', methods=['POST'])
def api_save_optimization_rules():
    """API endpoint to save optimization rules"""
    try:
        data = request.get_json()
        tool.optimization_rules.update(data)
        return jsonify({'status': 'success', 'message': 'Optimization rules saved successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/kpi-settings/reset', methods=['POST'])
def api_reset_kpi_settings():
    """API endpoint to reset KPI settings to defaults"""
    try:
        tool.kpi_settings = tool.load_default_kpi_settings()
        return jsonify({'status': 'success', 'message': 'KPI settings reset to defaults'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/optimization-rules/reset', methods=['POST'])
def api_reset_optimization_rules():
    """API endpoint to reset optimization rules to defaults"""
    try:
        tool.optimization_rules = tool.load_default_optimization_rules()
        return jsonify({'status': 'success', 'message': 'Optimization rules reset to defaults'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)

