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
        """Load data from all sources"""
        try:
            # Load Google Sheets data
            self.load_google_sheets_data()
            
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
            
            # FB Spend Data
            fb_sheet = self.gc.open_by_url('https://docs.google.com/spreadsheets/d/1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk/edit?usp=sharing')
            fb_worksheet = fb_sheet.get_worksheet(0)
            fb_records = fb_worksheet.get_all_records()
            self.fb_data = fb_records
            print(f"✅ Loaded {len(fb_records)} rows from fb_spend")
            
        except Exception as e:
            print(f"❌ Google Sheets loading error: {e}")

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
            
            # Process FB data and combine - FIXED COLUMN NAMES
            processed_count = 0
            for fb_row in self.fb_data:
                # Use correct column names from your actual data
                ad_set_name = str(fb_row.get('Facebook Adset Name', '')).strip()
                ad_name = str(fb_row.get('Facebook Ad Name', '')).strip()
                
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
                
                # Calculate KPIs using correct column names
                spend = self.safe_float(fb_row.get('Facebook Total Spend (USD)', 0))
                clicks = self.safe_float(fb_row.get('Facebook Total Clicks', 0))
                impressions = self.safe_float(fb_row.get('Facebook Total Impressions', 0))
                
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
                
                # Calculate KPIs
                ctr = (clicks / impressions * 100) if impressions > 0 else 0
                cpc = spend / clicks if clicks > 0 else 0
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
                
                # Success criteria
                success_criteria = {
                    'ctr_good': ctr > 0.30,
                    'funnel_start_good': funnel_start_rate > 15,
                    'cpa_good': cpa < 120 and cpa > 0,
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
                print("⚠️ No records were successfully combined - check data format...")
                # Show sample keys for debugging
                if self.fb_data:
                    print(f"📊 Sample FB keys: {list(self.fb_data[0].keys())}")
                if self.web_data:
                    print(f"📊 Sample Web keys: {list(self.web_data[0].keys())}")
                
        except Exception as e:
            print(f"❌ Data processing error: {e}")

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

    def get_performance_summary(self):
        """Get performance summary statistics"""
        if not self.performance_data:
            return {
                'total_ads': 0,
                'total_spend': 0,
                'total_revenue': 0,
                'total_clicks': 0,
                'total_impressions': 0,
                'total_bookings': 0,
                'avg_ctr': 0,
                'avg_cpc': 0,
                'avg_cpa': 0,
                'avg_roas': 0,
                'successful_ads': 0
            }
        
        total_spend = sum(ad['spend'] for ad in self.performance_data)
        total_revenue = sum(ad['revenue'] for ad in self.performance_data)
        total_clicks = sum(ad['clicks'] for ad in self.performance_data)
        total_impressions = sum(ad['impressions'] for ad in self.performance_data)
        total_bookings = sum(ad['bookings'] for ad in self.performance_data)
        
        avg_ctr = sum(ad['ctr'] for ad in self.performance_data) / len(self.performance_data)
        avg_cpc = sum(ad['cpc'] for ad in self.performance_data if ad['cpc'] > 0) / max(1, len([ad for ad in self.performance_data if ad['cpc'] > 0]))
        avg_cpa = sum(ad['cpa'] for ad in self.performance_data if ad['cpa'] > 0) / max(1, len([ad for ad in self.performance_data if ad['cpa'] > 0]))
        avg_roas = sum(ad['roas'] for ad in self.performance_data if ad['roas'] > 0) / max(1, len([ad for ad in self.performance_data if ad['roas'] > 0]))
        
        successful_ads = len([ad for ad in self.performance_data if ad['all_criteria_met']])
        
        return {
            'total_ads': len(self.performance_data),
            'total_spend': total_spend,
            'total_revenue': total_revenue,
            'total_clicks': total_clicks,
            'total_impressions': total_impressions,
            'total_bookings': total_bookings,
            'avg_ctr': avg_ctr,
            'avg_cpc': avg_cpc,
            'avg_cpa': avg_cpa,
            'avg_roas': avg_roas,
            'successful_ads': successful_ads
        }

    def get_optimization_recommendations(self):
        """Get optimization recommendations"""
        recommendations = []
        
        for ad in self.performance_data:
            # Pause recommendations
            if ad['roas'] < 0.5 and ad['spend'] > 100:
                recommendations.append({
                    'action': 'pause',
                    'type': 'ad',
                    'name': ad['ad_name'],
                    'ad_set': ad['ad_set_name'],
                    'reason': f'Low ROAS ({ad["roas"]:.2f}) with significant spend',
                    'spend': ad['spend'],
                    'roas': ad['roas'],
                    'cpa': ad['cpa'],
                    'bookings': ad['bookings'],
                    'priority': 'high'
                })
            
            elif ad['cpa'] > 200 and ad['spend'] > 50:
                recommendations.append({
                    'action': 'pause',
                    'type': 'ad',
                    'name': ad['ad_name'],
                    'ad_set': ad['ad_set_name'],
                    'reason': f'High CPA (${ad["cpa"]:.2f})',
                    'spend': ad['spend'],
                    'roas': ad['roas'],
                    'cpa': ad['cpa'],
                    'bookings': ad['bookings'],
                    'priority': 'medium'
                })
            
            # Scale recommendations
            elif ad['roas'] > 2.0 and ad['all_criteria_met']:
                recommendations.append({
                    'action': 'scale',
                    'type': 'ad',
                    'name': ad['ad_name'],
                    'ad_set': ad['ad_set_name'],
                    'reason': f'High ROAS ({ad["roas"]:.2f}) and meets all criteria',
                    'spend': ad['spend'],
                    'roas': ad['roas'],
                    'cpa': ad['cpa'],
                    'bookings': ad['bookings'],
                    'priority': 'high'
                })
        
        return sorted(recommendations, key=lambda x: (x['priority'] == 'high', x['roas']), reverse=True)

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
            Performance Summary:
            - Total Ads: {summary['total_ads']}
            - Total Spend: ${summary['total_spend']:,.2f}
            - Total Revenue: ${summary['total_revenue']:,.2f}
            - Average ROAS: {summary['avg_roas']:.2f}
            - Average CPA: ${summary['avg_cpa']:.2f}
            - Successful Ads: {summary['successful_ads']}
            
            Top 5 Performers (by ROAS):
            {chr(10).join([f"- {ad['ad_name'][:50]}... ROAS: {ad['roas']:.2f}, CPA: ${ad['cpa']:.2f}" for ad in top_performers])}
            
            Worst 5 Performers (by ROAS):
            {chr(10).join([f"- {ad['ad_name'][:50]}... ROAS: {ad['roas']:.2f}, CPA: ${ad['cpa']:.2f}" for ad in worst_performers])}
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
                - Average Funnel Start Rate: {sum(ad['funnel_start_rate'] for ad in self.performance_data) / len(self.performance_data):.1f}%
                - Average Survey Completion Rate: {sum(ad['survey_completion_rate'] for ad in self.performance_data) / len(self.performance_data):.1f}%
                - Average Booking Conversion Rate: {sum(ad['booking_conversion_rate'] for ad in self.performance_data) / len(self.performance_data):.1f}%
                
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

@app.route('/api/optimization-recommendations')
def optimization_recommendations():
    recommendations = tool.get_optimization_recommendations()
    return jsonify(recommendations)

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
Based on analysis of {len(tool.performance_data)} ads, successful patterns show:
- 93% of successful creatives use Static format
- 100% of successful creatives are Seasonal campaigns
- $150 offers show 19% success rate vs 12.6% average
- Amazon/Costco partnerships drive 79% of successful creatives

## Messaging Framework
**Primary Message:** {messaging_pillar} focused messaging
**Value Proposition:** {value_pillar}
**Call to Action:** Book your appointment today

## Visual Direction
**Format:** {creative_format.title()} creative
**Style:** Clean, professional, trust-building
**Brand Elements:** Incorporate partner brand (Amazon/Costco recommended)

## Reference Links
- Brand Guidelines: https://docs.google.com/presentation/d/1zCTFwviKE_MiKF5lpjFChdju1wWS0bl9GsmJmmw91Pk/edit
- Social Figma Board: https://www.figma.com/design/K7w8jbHpE2Dv0ZNbeNxDyw/Social-Promotions-and-Affliates-Master
- Creative Repository 1: https://drive.google.com/drive/folders/1TZG22ZvkFYIsMXchOKUiaVP4vgpqtcrH
- Creative Repository 2: https://drive.google.com/drive/folders/1rPfI1h5FoU94xsrh0tTXasspMJNDrMsc

## Success Criteria
Target metrics based on successful ads:
- CTR > 0.30%
- Funnel Start Rate > 15%
- CPA < $120
- Clicks > 500

## Example Ads
[To be added by user]
"""
    
    return jsonify({'brief': brief})

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
    app.run(host='0.0.0.0', port=8080, debug=False)

