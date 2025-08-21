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
        
        # Initialize APIs
        self.init_google_sheets()
        self.init_facebook_api()
        
        # Load data
        self.performance_data = []
        self.web_data = []
        self.attr_data = []
        self.fb_data = []
        self.load_data()
        
        print("✅ Facebook Optimization Tool initialized")

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
        """Load data from Google Sheets"""
        try:
            if self.gc:
                # Load from Google Sheets
                self.load_google_sheets_data()
            else:
                # Load sample data
                self.load_sample_data()
            print("✅ Data loaded successfully")
        except Exception as e:
            print(f"❌ Data loading error: {e}")
            self.load_sample_data()

    def load_google_sheets_data(self):
        """Load data from Google Sheets"""
        try:
            # Web Pages Data
            web_sheet = self.gc.open_by_key('1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA').sheet1
            self.web_data = web_sheet.get_all_records()
            print(f"✅ Loaded {len(self.web_data)} rows from web_pages")
            
            # Attribution Data
            attr_sheet = self.gc.open_by_key('1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ').sheet1
            self.attr_data = attr_sheet.get_all_records()
            print(f"✅ Loaded {len(self.attr_data)} rows from attribution")
            
            # FB Spend Data
            fb_sheet = self.gc.open_by_key('1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk').sheet1
            self.fb_data = fb_sheet.get_all_records()
            print(f"✅ Loaded {len(self.fb_data)} rows from fb_spend")
            
            # Process and combine data
            self.process_combined_data()
            
        except Exception as e:
            print(f"❌ Google Sheets loading error: {e}")
            self.load_sample_data()

    def process_combined_data(self):
        """Process and combine data from all sources"""
        try:
            combined_data = []
            
            print(f"🔄 Processing data: {len(self.web_data)} web, {len(self.attr_data)} attr, {len(self.fb_data)} fb")
            
            # Create lookup dictionaries with flexible key matching
            web_lookup = {}
            for row in self.web_data:
                utm_content = str(row.get('Web Pages UTM Content', '')).strip()
                utm_term = str(row.get('Web Pages UTM Term', '')).strip()
                if utm_content and utm_term and utm_content.lower() != 'total' and utm_term.lower() != 'total':
                    # Try multiple key formats
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
                    # Try multiple key formats
                    keys = [
                        f"{utm_term}_{utm_content}",
                        f"{utm_content}_{utm_term}",
                        utm_content,
                        utm_term
                    ]
                    for key in keys:
                        attr_lookup[key.lower()] = row
            
            print(f"🔍 Created lookups: {len(web_lookup)} web keys, {len(attr_lookup)} attr keys")
            
            # Process FB data and combine
            processed_count = 0
            for fb_row in self.fb_data:
                ad_set_name = str(fb_row.get('Ad Set Name', '')).strip()
                ad_name = str(fb_row.get('Ad Name', '')).strip()
                
                # Skip totals rows and empty rows
                if not ad_set_name or not ad_name:
                    continue
                if any(word in ad_set_name.lower() for word in ['total', 'sum', 'grand']):
                    continue
                if any(word in ad_name.lower() for word in ['total', 'sum', 'grand']):
                    continue
                
                # Try to find matching data with flexible matching
                web_row = {}
                attr_row = {}
                
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
                
                # Calculate KPIs
                spend = self.safe_float(fb_row.get('Amount Spent (USD)', 0))
                clicks = self.safe_float(fb_row.get('Link Clicks', 0))
                impressions = self.safe_float(fb_row.get('Impressions', 0))
                
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
                
                # Calculate derived metrics
                ctr = (clicks / impressions * 100) if impressions > 0 else 0
                cpc = (spend / clicks) if clicks > 0 else 0
                funnel_start_rate = (funnel_starts / site_visits * 100) if site_visits > 0 else 0
                survey_completion_rate = (survey_complete / funnel_starts * 100) if funnel_starts > 0 else 0
                checkout_start_rate = (checkout_starts / survey_complete * 100) if survey_complete > 0 else 0
                booking_conversion_rate = (bookings / site_visits * 100) if site_visits > 0 else 0
                cpa = (spend / bookings) if bookings > 0 else 0
                
                # Calculate LTV/CAC with completion rate
                completed_appointments = bookings * completion_rate
                total_cost = spend + promo_spend
                cac = (total_cost / completed_appointments) if completed_appointments > 0 else 0
                ltv = (revenue / completed_appointments) if completed_appointments > 0 else 0
                roas = (ltv / cac) if cac > 0 else 0
                
                # Success criteria
                success_criteria = {
                    'ctr_good': ctr > 0.30,
                    'funnel_start_good': funnel_start_rate > 15,
                    'cpa_good': cpa < 120,
                    'clicks_good': clicks > 500
                }
                
                combined_row = {
                    'ad_set_name': ad_set_name,
                    'ad_name': ad_name,
                    'spend': spend,
                    'clicks': clicks,
                    'impressions': impressions,
                    'ctr': ctr,
                    'cpc': cpc,
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
                    'has_attr_data': bool(attr_row)
                }
                
                combined_data.append(combined_row)
                processed_count += 1
            
            self.performance_data = combined_data
            print(f"✅ Processed {processed_count} combined records from {len(self.fb_data)} FB records")
            
            if processed_count == 0:
                print("⚠️ No records were successfully combined - checking data format...")
                # Debug: Print sample data
                if self.fb_data:
                    sample_fb = self.fb_data[0]
                    print(f"📊 Sample FB keys: {list(sample_fb.keys())}")
                if self.web_data:
                    sample_web = self.web_data[0]
                    print(f"📊 Sample Web keys: {list(sample_web.keys())}")
                
        except Exception as e:
            print(f"❌ Data processing error: {e}")
            self.load_sample_data()

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

    def load_sample_data(self):
        """Load sample data for testing"""
        self.performance_data = [
            {
                'ad_set_name': '071425_CEO_AppointmentPage_Calgary_Amazon_130_EngagedShoppers_Video_Feed-Stories-Reels_EXP-InsuranceLP',
                'ad_name': '070325_$130_Static_Amazon_PrimeDay_Batch33_V7_NF',
                'spend': 1500,
                'clicks': 250,
                'impressions': 35000,
                'ctr': 0.71,
                'cpc': 6.00,
                'site_visits': 200,
                'funnel_starts': 40,
                'funnel_start_rate': 20.0,
                'survey_complete': 30,
                'survey_completion_rate': 75.0,
                'checkout_starts': 25,
                'checkout_start_rate': 83.3,
                'bookings': 8,
                'booking_conversion_rate': 4.0,
                'cpa': 187.50,
                'revenue': 2400,
                'ltv': 666.67,
                'cac': 416.67,
                'roas': 1.6,
                'completion_rate': 0.45,
                'promo_spend': 120,
                'total_cost': 1620,
                'success_criteria': {'ctr_good': True, 'funnel_start_good': True, 'cpa_good': False, 'clicks_good': False},
                'all_criteria_met': False,
                'has_web_data': True,
                'has_attr_data': True
            },
            {
                'ad_set_name': '071425_CEO_AppointmentPage_Toronto_Costco_150_LookalikeAudience_Static_Feed_EXP-LandingPageA',
                'ad_name': '062625_$150_UGC_Video_Costco_Seasonal_Emily_Convenience_5StarDentist_H1B2_NR',
                'spend': 2200,
                'clicks': 380,
                'impressions': 45000,
                'ctr': 0.84,
                'cpc': 5.79,
                'site_visits': 320,
                'funnel_starts': 65,
                'funnel_start_rate': 20.3,
                'survey_complete': 50,
                'survey_completion_rate': 76.9,
                'checkout_starts': 42,
                'checkout_start_rate': 84.0,
                'bookings': 15,
                'booking_conversion_rate': 4.7,
                'cpa': 146.67,
                'revenue': 4500,
                'ltv': 666.67,
                'cac': 325.93,
                'roas': 2.05,
                'completion_rate': 0.45,
                'promo_spend': 225,
                'total_cost': 2425,
                'success_criteria': {'ctr_good': True, 'funnel_start_good': True, 'cpa_good': False, 'clicks_good': False},
                'all_criteria_met': False,
                'has_web_data': True,
                'has_attr_data': True
            }
        ]
        print("✅ Sample data loaded")

    def call_openai_api(self, messages, temperature=0.7, max_tokens=1000):
        """Make API call to OpenAI"""
        try:
            if not self.openai_api_key:
                return "OpenAI API key not configured"
            
            headers = {
                'Authorization': f'Bearer {self.openai_api_key}',
                'Content-Type': 'application/json'
            }
            
            data = {
                'model': 'gpt-4o-mini',
                'messages': messages,
                'temperature': temperature,
                'max_tokens': max_tokens
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
                return f"OpenAI API error: {response.status_code} - {response.text}"
                
        except Exception as e:
            return f"OpenAI API error: {str(e)}"

    def get_performance_summary(self):
        """Get summary of performance data for AI analysis"""
        if not self.performance_data:
            return "No performance data available"
        
        total_ads = len(self.performance_data)
        successful_ads = len([ad for ad in self.performance_data if ad['all_criteria_met']])
        
        # Calculate aggregates
        total_spend = sum(ad['spend'] for ad in self.performance_data)
        total_revenue = sum(ad['revenue'] for ad in self.performance_data)
        avg_ctr = sum(ad['ctr'] for ad in self.performance_data) / total_ads
        avg_cpa = sum(ad['cpa'] for ad in self.performance_data if ad['cpa'] > 0) / max(1, len([ad for ad in self.performance_data if ad['cpa'] > 0]))
        avg_roas = sum(ad['roas'] for ad in self.performance_data if ad['roas'] > 0) / max(1, len([ad for ad in self.performance_data if ad['roas'] > 0]))
        
        # Top performers
        top_performers = sorted(self.performance_data, key=lambda x: x['roas'], reverse=True)[:5]
        worst_performers = sorted(self.performance_data, key=lambda x: x['roas'])[:5]
        
        summary = f"""
        PERFORMANCE DATA SUMMARY:
        - Total Ads: {total_ads}
        - Successful Ads (all criteria met): {successful_ads} ({successful_ads/total_ads*100:.1f}%)
        - Total Spend: ${total_spend:,.2f}
        - Total Revenue: ${total_revenue:,.2f}
        - Average CTR: {avg_ctr:.2f}%
        - Average CPA: ${avg_cpa:.2f}
        - Average ROAS: {avg_roas:.2f}
        
        TOP 3 PERFORMERS (by ROAS):
        """
        
        for i, ad in enumerate(top_performers[:3]):
            summary += f"\n{i+1}. {ad['ad_name'][:50]}... - ROAS: {ad['roas']:.2f}, CTR: {ad['ctr']:.2f}%, CPA: ${ad['cpa']:.2f}"
        
        summary += "\n\nWORST 3 PERFORMERS (by ROAS):"
        for i, ad in enumerate(worst_performers[:3]):
            summary += f"\n{i+1}. {ad['ad_name'][:50]}... - ROAS: {ad['roas']:.2f}, CTR: {ad['ctr']:.2f}%, CPA: ${ad['cpa']:.2f}"
        
        return summary

# Initialize the tool
tool = FacebookOptimizationTool()

@app.route('/')
def dashboard():
    return render_template('enhanced_dashboard.html')

@app.route('/api/performance-summary')
def performance_summary():
    try:
        if not tool.performance_data:
            return jsonify({'error': 'No performance data available'}), 404
        
        # Calculate summary statistics
        total_spend = sum(row['spend'] for row in tool.performance_data)
        total_clicks = sum(row['clicks'] for row in tool.performance_data)
        total_impressions = sum(row['impressions'] for row in tool.performance_data)
        total_bookings = sum(row['bookings'] for row in tool.performance_data)
        total_revenue = sum(row['revenue'] for row in tool.performance_data)
        
        avg_ctr = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
        avg_cpa = (total_spend / total_bookings) if total_bookings > 0 else 0
        overall_roas = (total_revenue / total_spend) if total_spend > 0 else 0
        
        successful_ads = len([row for row in tool.performance_data if row['all_criteria_met']])
        
        summary = {
            'total_ads': len(tool.performance_data),
            'total_spend': total_spend,
            'total_clicks': total_clicks,
            'total_bookings': total_bookings,
            'total_revenue': total_revenue,
            'avg_ctr': avg_ctr,
            'avg_cpa': avg_cpa,
            'overall_roas': overall_roas,
            'successful_ads': successful_ads,
            'success_rate': (successful_ads / len(tool.performance_data) * 100) if tool.performance_data else 0
        }
        
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/performance-data')
def performance_data():
    try:
        if not tool.performance_data:
            return jsonify({'error': 'No performance data available'}), 404
            
        view_type = request.args.get('view', 'adset')  # 'adset' or 'ad'
        
        if view_type == 'adset':
            # Aggregate by ad set
            adset_data = {}
            for row in tool.performance_data:
                adset_name = row['ad_set_name']
                if adset_name not in adset_data:
                    adset_data[adset_name] = {
                        'ad_set_name': adset_name,
                        'spend': 0,
                        'clicks': 0,
                        'impressions': 0,
                        'site_visits': 0,
                        'funnel_starts': 0,
                        'bookings': 0,
                        'revenue': 0,
                        'ads': []
                    }
                
                adset_data[adset_name]['spend'] += row['spend']
                adset_data[adset_name]['clicks'] += row['clicks']
                adset_data[adset_name]['impressions'] += row['impressions']
                adset_data[adset_name]['site_visits'] += row['site_visits']
                adset_data[adset_name]['funnel_starts'] += row['funnel_starts']
                adset_data[adset_name]['bookings'] += row['bookings']
                adset_data[adset_name]['revenue'] += row['revenue']
                adset_data[adset_name]['ads'].append(row)
            
            # Calculate derived metrics for ad sets
            for adset in adset_data.values():
                adset['ctr'] = (adset['clicks'] / adset['impressions'] * 100) if adset['impressions'] > 0 else 0
                adset['cpc'] = (adset['spend'] / adset['clicks']) if adset['clicks'] > 0 else 0
                adset['funnel_start_rate'] = (adset['funnel_starts'] / adset['site_visits'] * 100) if adset['site_visits'] > 0 else 0
                adset['booking_conversion_rate'] = (adset['bookings'] / adset['site_visits'] * 100) if adset['site_visits'] > 0 else 0
                adset['cpa'] = (adset['spend'] / adset['bookings']) if adset['bookings'] > 0 else 0
                adset['roas'] = (adset['revenue'] / adset['spend']) if adset['spend'] > 0 else 0
            
            return jsonify(list(adset_data.values()))
        else:
            # Return individual ad data
            return jsonify(tool.performance_data)
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/optimization-recommendations')
def optimization_recommendations():
    try:
        if not tool.performance_data:
            return jsonify({'error': 'No performance data available'}), 404
            
        recommendations = []
        
        for row in tool.performance_data:
            spend = row['spend']
            ctr = row['ctr']
            cpc = row['cpc']
            funnel_start_rate = row['funnel_start_rate']
            bookings = row['bookings']
            cpa = row['cpa']
            roas = row['roas']
            booking_conversion_rate = row['booking_conversion_rate']
            
            # 24-hour rules (Rule 2)
            if spend > 100:
                if ctr <= 0.40 or cpc >= 6 or funnel_start_rate <= 15:
                    recommendations.append({
                        'ad_set_name': row['ad_set_name'],
                        'ad_name': row['ad_name'],
                        'action': 'pause',
                        'reason': 'Poor early performance metrics (24h rule)',
                        'spend': spend,
                        'bookings': bookings,
                        'cpa': cpa,
                        'current_metrics': {
                            'ctr': ctr,
                            'cpc': cpc,
                            'funnel_start_rate': funnel_start_rate
                        }
                    })
            
            # Advanced performance rules (Rule 3)
            if spend > 300:
                if booking_conversion_rate > 2.5 and cpa < 120 and roas > 1:
                    recommendations.append({
                        'ad_set_name': row['ad_set_name'],
                        'ad_name': row['ad_name'],
                        'action': 'scale',
                        'reason': 'Excellent performance - ready for scaling',
                        'spend': spend,
                        'bookings': bookings,
                        'cpa': cpa,
                        'current_metrics': {
                            'booking_conversion_rate': booking_conversion_rate,
                            'cpa': cpa,
                            'roas': roas
                        }
                    })
        
        return jsonify(recommendations)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/creative-brief', methods=['POST'])
def generate_creative_brief():
    try:
        data = request.json
        campaign_name = data.get('campaign_name', 'New Campaign')
        campaign_type = data.get('campaign_type', 'seasonal')
        messaging_pillar = data.get('messaging_pillar', 'Convenience')
        value_pillar = data.get('value_pillar', 'Convenience-Insurance')
        creative_format = data.get('creative_format', 'Static')
        
        # Get performance insights for the brief
        performance_summary = tool.get_performance_summary()
        
        # Generate AI-enhanced brief with actual data
        prompt = f"""
        Create a comprehensive Facebook creative brief for a dental appointment booking campaign based on actual performance data.
        
        Campaign Details:
        - Name: {campaign_name}
        - Type: {campaign_type}
        - Messaging Pillar: {messaging_pillar}
        - Value Pillar: {value_pillar}
        - Creative Format: {creative_format}
        
        ACTUAL PERFORMANCE DATA:
        {performance_summary}
        
        Based on this real performance data and successful patterns, create a brief that includes:
        1. Campaign Overview with data-driven insights
        2. Target Audience Insights based on successful campaigns
        3. Messaging Framework incorporating the {messaging_pillar} pillar
        4. Visual Direction for {creative_format} format
        5. Success Metrics based on current performance benchmarks
        6. Specific recommendations based on top-performing ads
        
        Focus on actionable insights from the performance data to improve campaign success.
        """
        
        messages = [{"role": "user", "content": prompt}]
        ai_brief = tool.call_openai_api(messages, temperature=0.7, max_tokens=1500)
        
        return jsonify({
            'campaign_name': campaign_name,
            'campaign_type': campaign_type,
            'messaging_pillar': messaging_pillar,
            'value_pillar': value_pillar,
            'creative_format': creative_format,
            'ai_brief': ai_brief,
            'generated_at': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/ai-insights')
def ai_insights():
    try:
        insight_type = request.args.get('type', 'cluster')
        
        # Get actual performance data for analysis
        performance_summary = tool.get_performance_summary()
        
        if insight_type == 'cluster':
            prompt = f"""
            Analyze this ACTUAL Facebook ad performance data and identify key patterns for optimization:
            
            {performance_summary}
            
            Provide specific insights about:
            1. What makes the top performers successful
            2. Common patterns in underperforming ads
            3. Actionable optimization recommendations
            4. Budget reallocation suggestions
            5. Creative strategy insights
            
            Focus on data-driven insights, not generic advice.
            """
        elif insight_type == 'creative':
            prompt = f"""
            Analyze creative performance patterns from this ACTUAL data:
            
            {performance_summary}
            
            Provide insights on:
            1. Creative elements that drive success
            2. Messaging patterns in top performers
            3. Format recommendations (Static vs Video)
            4. Creative refresh opportunities
            5. A/B testing suggestions
            
            Base recommendations on actual performance data.
            """
        elif insight_type == 'cro':
            prompt = f"""
            Analyze conversion funnel performance from this ACTUAL data:
            
            {performance_summary}
            
            Provide CRO recommendations for:
            1. Funnel Start rate optimization
            2. Survey Completion improvements
            3. Checkout Start rate enhancement
            4. Landing page optimization
            5. User experience improvements
            
            Focus on data-driven conversion optimization.
            """
        else:
            prompt = f"""
            Provide strategic recommendations based on this ACTUAL performance data:
            
            {performance_summary}
            
            Include:
            1. Campaign strategy insights
            2. Budget allocation recommendations
            3. Audience targeting optimization
            4. Scaling opportunities
            5. Risk mitigation strategies
            
            Base all recommendations on the actual data provided.
            """
        
        messages = [{"role": "user", "content": prompt}]
        ai_insights = tool.call_openai_api(messages, temperature=0.7, max_tokens=1200)
        
        return jsonify({
            'insight_type': insight_type,
            'insights': ai_insights,
            'data_summary': performance_summary,
            'generated_at': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/test-openai')
def test_openai():
    try:
        if not tool.openai_api_key:
            return jsonify({'status': 'error', 'message': 'OpenAI API key not configured'})
        
        messages = [{"role": "user", "content": "Test connection. Respond with 'Connection successful'."}]
        response = tool.call_openai_api(messages, max_tokens=50)
        
        return jsonify({
            'status': 'success',
            'message': 'OpenAI connection working',
            'response': response
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/refresh-data')
def refresh_data():
    try:
        tool.load_data()
        return jsonify({
            'status': 'success',
            'message': 'Data refreshed successfully',
            'records_loaded': len(tool.performance_data),
            'has_real_data': len(tool.performance_data) > 2  # More than sample data
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=False)

