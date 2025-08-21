from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import pandas as pd
import numpy as np
import json
import os
import requests
from datetime import datetime, timedelta
import time
import threading
from apscheduler.schedulers.background import BackgroundScheduler
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

class FacebookOptimizationTool:
    def __init__(self):
        self.data = {}
        self.performance_summary = {}
        self.creative_analysis = {}
        self.last_refresh = None
        self.scheduler = BackgroundScheduler()
        
        # Initialize APIs
        self.init_openai()
        self.init_google_sheets()
        self.init_facebook_api()
        
        # Load initial data
        self.load_data()
        
        # Start automation
        self.start_automation()
    
    def init_openai(self):
        """Initialize OpenAI API"""
        try:
            self.openai_api_key = os.getenv('OPENAI_API_KEY')
            if self.openai_api_key:
                logger.info("✅ OpenAI API key configured")
            else:
                logger.warning("⚠️ OpenAI API key not found")
        except Exception as e:
            logger.error(f"❌ OpenAI initialization error: {e}")
    
    def init_google_sheets(self):
        """Initialize Google Sheets API"""
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            
            credentials_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if credentials_json:
                credentials_dict = json.loads(credentials_json)
                credentials = Credentials.from_service_account_info(
                    credentials_dict,
                    scopes=['https://www.googleapis.com/auth/spreadsheets',
                           'https://www.googleapis.com/auth/drive']
                )
                self.gc = gspread.authorize(credentials)
                logger.info("✅ Google Sheets API initialized")
            else:
                logger.warning("⚠️ Google Sheets credentials not found")
                self.gc = None
        except Exception as e:
            logger.error(f"❌ Google Sheets initialization error: {e}")
            self.gc = None
    
    def init_facebook_api(self):
        """Initialize Facebook Marketing API"""
        try:
            self.fb_access_token = os.getenv('FB_ACCESS_TOKEN')
            self.fb_ad_account_id = os.getenv('FB_AD_ACCOUNT_ID')
            
            if self.fb_access_token and self.fb_ad_account_id:
                logger.info("✅ Facebook API credentials configured")
            else:
                logger.warning("⚠️ Facebook API credentials not found")
        except Exception as e:
            logger.error(f"❌ Facebook API initialization error: {e}")
    
    def safe_convert_to_number(self, value, default=0):
        """Safely convert value to number"""
        try:
            if value is None or value == '':
                return default
            if isinstance(value, list):
                return float(value[0]) if value else default
            if isinstance(value, str):
                # Remove commas and currency symbols
                cleaned = value.replace(',', '').replace('$', '').replace('%', '')
                return float(cleaned) if cleaned else default
            return float(value)
        except (ValueError, TypeError):
            return default
    
    def load_data(self):
        """Load data from all sources"""
        try:
            # Load from Google Sheets
            if self.gc:
                self.load_google_sheets_data()
            
            # Load from Facebook API
            self.load_facebook_data()
            
            # Process and combine data
            self.process_data()
            
            self.last_refresh = datetime.now()
            logger.info("✅ Data loaded successfully")
            
        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            # Load sample data as fallback
            self.load_sample_data()
    
    def load_google_sheets_data(self):
        """Load data from Google Sheets"""
        try:
            # Web Pages Data
            web_pages_sheet = self.gc.open_by_key('1e_eimaB0WTMOcWalCwSnMGFCZ5fDG1y7jpZF-qBNfdA').sheet1
            web_pages_data = web_pages_sheet.get_all_records()
            logger.info(f"✅ Loaded {len(web_pages_data)} rows from web_pages")
            
            # Attribution Data
            attribution_sheet = self.gc.open_by_key('1k49FsG1hAO3L-CGq1UjBPUxuDA6ZLMX0FCSMJQzmUCQ').sheet1
            attribution_data = attribution_sheet.get_all_records()
            logger.info(f"✅ Loaded {len(attribution_data)} rows from attribution")
            
            # FB Spend Data
            fb_spend_sheet = self.gc.open_by_key('1BG--tds9na-WC3Dx3t0DTuWcmZAVYbBsvWCUJ-yFQTk').sheet1
            fb_spend_data = fb_spend_sheet.get_all_records()
            logger.info(f"✅ Loaded {len(fb_spend_data)} rows from fb_spend")
            
            # Store raw data
            self.data['web_pages'] = web_pages_data
            self.data['attribution'] = attribution_data
            self.data['fb_spend'] = fb_spend_data
            
        except Exception as e:
            logger.error(f"❌ Error loading Google Sheets data: {e}")
    
    def load_facebook_data(self):
        """Load creative data from Facebook API"""
        try:
            if not self.fb_access_token or not self.fb_ad_account_id:
                logger.warning("⚠️ Facebook API not configured")
                return
            
            # Date range: June 1, 2025 onwards
            since_date = '2025-06-01'
            
            # Get ads with creative data
            url = f"https://graph.facebook.com/v18.0/{self.fb_ad_account_id}/ads"
            params = {
                'access_token': self.fb_access_token,
                'fields': 'id,name,creative{title,body,object_story_spec},insights{spend,clicks,impressions,actions,ctr,cpc}',
                'time_range': f'{{"since":"{since_date}"}}',
                'limit': 500
            }
            
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                fb_data = response.json()
                ads = fb_data.get('data', [])
                
                # Process creative data
                creative_data = []
                for ad in ads:
                    try:
                        creative_info = {
                            'ad_id': ad.get('id'),
                            'ad_name': ad.get('name', ''),
                            'primary_text': '',
                            'headline': '',
                            'spend': 0,
                            'clicks': 0,
                            'impressions': 0,
                            'ctr': 0,
                            'cpc': 0,
                            'conversions': 0
                        }
                        
                        # Extract creative copy
                        creative = ad.get('creative', {})
                        if 'body' in creative:
                            creative_info['primary_text'] = creative['body']
                        if 'title' in creative:
                            creative_info['headline'] = creative['title']
                        
                        # Extract object story spec
                        object_story = creative.get('object_story_spec', {})
                        if 'link_data' in object_story:
                            link_data = object_story['link_data']
                            creative_info['primary_text'] = link_data.get('message', creative_info['primary_text'])
                            creative_info['headline'] = link_data.get('name', creative_info['headline'])
                        
                        # Extract insights
                        insights = ad.get('insights', {}).get('data', [])
                        if insights:
                            insight = insights[0]
                            creative_info['spend'] = self.safe_convert_to_number(insight.get('spend'))
                            creative_info['clicks'] = self.safe_convert_to_number(insight.get('clicks'))
                            creative_info['impressions'] = self.safe_convert_to_number(insight.get('impressions'))
                            creative_info['ctr'] = self.safe_convert_to_number(insight.get('ctr'))
                            creative_info['cpc'] = self.safe_convert_to_number(insight.get('cpc'))
                            
                            # Extract conversions from actions
                            actions = insight.get('actions', [])
                            if isinstance(actions, list):
                                for action in actions:
                                    if action.get('action_type') == 'offsite_conversion':
                                        creative_info['conversions'] = self.safe_convert_to_number(action.get('value'))
                        
                        creative_data.append(creative_info)
                        
                    except Exception as e:
                        logger.warning(f"Error processing ad {ad.get('id')}: {e}")
                        continue
                
                self.data['facebook_creative'] = creative_data
                logger.info(f"✅ Fetched {len(creative_data)} ads from Facebook API")
                
            else:
                logger.error(f"❌ Facebook API error: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"❌ Facebook API error: {e}")
    
    def load_sample_data(self):
        """Load sample data for demo purposes"""
        sample_data = []
        for i in range(50):
            sample_data.append({
                'ad_name': f'Sample_Ad_{i+1}',
                'ad_set_name': f'Sample_AdSet_{(i//5)+1}',
                'spend': np.random.uniform(50, 500),
                'clicks': np.random.randint(100, 1500),
                'impressions': np.random.randint(5000, 45000),
                'conversions': np.random.randint(1, 40),
                'bookings': np.random.randint(1, 30),
                'revenue': np.random.uniform(100, 900),
                'ctr': np.random.uniform(0.1, 2.5),
                'cpc': np.random.uniform(1, 8),
                'cpa': np.random.uniform(50, 200),
                'roas': np.random.uniform(0.5, 3.5),
                'primary_text': f'Sample primary text for ad {i+1}',
                'headline': f'Sample headline {i+1}'
            })
        
        self.data['performance'] = sample_data
        logger.info("✅ Sample data loaded")
    
    def process_data(self):
        """Process and combine data from all sources"""
        try:
            if not self.data:
                return
            
            # Combine Google Sheets data
            performance_data = []
            
            # Process each data source
            web_pages = self.data.get('web_pages', [])
            attribution = self.data.get('attribution', [])
            fb_spend = self.data.get('fb_spend', [])
            facebook_creative = self.data.get('facebook_creative', [])
            
            # Create lookup dictionaries
            web_lookup = {(row.get('Web Pages UTM Content', ''), row.get('Web Pages UTM Term', '')): row for row in web_pages}
            attr_lookup = {(row.get('Attribution UTM Content', ''), row.get('Attribution UTM Term', '')): row for row in attribution}
            spend_lookup = {(row.get('Ad Name', ''), row.get('Ad Set Name', '')): row for row in fb_spend}
            creative_lookup = {row.get('ad_name', ''): row for row in facebook_creative}
            
            # Combine data
            all_keys = set()
            all_keys.update(web_lookup.keys())
            all_keys.update(attr_lookup.keys())
            all_keys.update(spend_lookup.keys())
            
            for ad_name, ad_set_name in all_keys:
                if not ad_name or not ad_set_name:
                    continue
                
                # Skip totals rows
                if any(term in str(ad_name).lower() for term in ['total', 'sum', 'grand']):
                    continue
                if any(term in str(ad_set_name).lower() for term in ['total', 'sum', 'grand']):
                    continue
                
                web_data = web_lookup.get((ad_name, ad_set_name), {})
                attr_data = attr_lookup.get((ad_name, ad_set_name), {})
                spend_data = spend_lookup.get((ad_name, ad_set_name), {})
                creative_data = creative_lookup.get(ad_name, {})
                
                # Calculate metrics
                site_visits = self.safe_convert_to_number(web_data.get('Web Pages Unique Count of Landing Pages', 0))
                funnel_starts = self.safe_convert_to_number(web_data.get('Web Pages Unique Count of Sessions with Funnel Starts', 0))
                survey_complete = self.safe_convert_to_number(web_data.get('Web Pages Unique Count of Sessions with Match Results', 0))
                checkout_starts = self.safe_convert_to_number(web_data.get('Count of Sessions with Checkout Started (V2 included)', 0))
                bookings = self.safe_convert_to_number(attr_data.get('Attribution Attributed NPRs', 0))
                
                completion_rate = self.safe_convert_to_number(attr_data.get('Attribution Attibuted PAS (Predicted)', 0.45))
                if completion_rate < 0.39 or completion_rate > 0.51:
                    completion_rate = 0.45
                
                revenue = self.safe_convert_to_number(attr_data.get('Attribution Attibuted Total Revenue (Predicted) (USD)', 0))
                promo_spend = self.safe_convert_to_number(attr_data.get('Attibuted Offer Spend (Predicted) (USD)', 0))
                
                # Facebook spend data
                ad_spend = self.safe_convert_to_number(spend_data.get('Amount Spent (USD)', 0))
                clicks = self.safe_convert_to_number(spend_data.get('Link Clicks', 0))
                impressions = self.safe_convert_to_number(spend_data.get('Impressions', 0))
                
                # Calculate derived metrics
                ctr = (clicks / impressions * 100) if impressions > 0 else 0
                cpc = (ad_spend / clicks) if clicks > 0 else 0
                funnel_start_rate = (funnel_starts / site_visits * 100) if site_visits > 0 else 0
                survey_completion_rate = (survey_complete / funnel_starts * 100) if funnel_starts > 0 else 0
                checkout_start_rate = (checkout_starts / survey_complete * 100) if survey_complete > 0 else 0
                booking_conversion_rate = (bookings / site_visits * 100) if site_visits > 0 else 0
                
                cpa = (ad_spend / bookings) if bookings > 0 else 0
                cac = ((ad_spend + promo_spend) / (bookings * completion_rate)) if (bookings * completion_rate) > 0 else 0
                ltv = (revenue / (bookings * completion_rate)) if (bookings * completion_rate) > 0 else 0
                roas = (ltv / cac) if cac > 0 else 0
                
                # Success criteria
                success_criteria = {
                    'ctr_success': ctr > 0.30,
                    'funnel_start_success': funnel_start_rate > 15,
                    'cpa_success': cpa < 120 and cpa > 0,
                    'clicks_success': clicks > 500
                }
                
                all_success = all(success_criteria.values())
                
                row = {
                    'ad_name': ad_name,
                    'ad_set_name': ad_set_name,
                    'spend': ad_spend,
                    'clicks': clicks,
                    'impressions': impressions,
                    'site_visits': site_visits,
                    'funnel_starts': funnel_starts,
                    'survey_complete': survey_complete,
                    'checkout_starts': checkout_starts,
                    'bookings': bookings,
                    'revenue': revenue,
                    'promo_spend': promo_spend,
                    'completion_rate': completion_rate,
                    'ctr': ctr,
                    'cpc': cpc,
                    'funnel_start_rate': funnel_start_rate,
                    'survey_completion_rate': survey_completion_rate,
                    'checkout_start_rate': checkout_start_rate,
                    'booking_conversion_rate': booking_conversion_rate,
                    'cpa': cpa,
                    'cac': cac,
                    'ltv': ltv,
                    'roas': roas,
                    'all_success': all_success,
                    'primary_text': creative_data.get('primary_text', ''),
                    'headline': creative_data.get('headline', ''),
                    **success_criteria
                }
                
                performance_data.append(row)
            
            self.data['performance'] = performance_data
            
            # Calculate summary statistics
            df = pd.DataFrame(performance_data)
            self.performance_summary = {
                'total_ads': len(df),
                'total_spend': df['spend'].sum(),
                'total_revenue': df['revenue'].sum(),
                'avg_roas': df['roas'].mean(),
                'avg_ctr': df['ctr'].mean(),
                'avg_cpa': df['cpa'].mean(),
                'successful_ads': df['all_success'].sum(),
                'high_performers': len(df[df['roas'] > 2.0]),
                'low_performers': len(df[df['roas'] < 0.5])
            }
            
            logger.info("✅ Data processed successfully")
            
        except Exception as e:
            logger.error(f"❌ Error processing data: {e}")
    
    def call_openai_api(self, messages, temperature=0.7, max_tokens=1500):
        """Make direct API call to OpenAI"""
        try:
            if not self.openai_api_key:
                return "OpenAI API not configured. Please add your OPENAI_API_KEY environment variable and restart the application."
            
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
            return f"Error calling OpenAI API: {str(e)}"
    
    def get_optimization_recommendations(self):
        """Generate optimization recommendations based on performance rules"""
        try:
            recommendations = []
            performance_data = self.data.get('performance', [])
            
            for row in performance_data:
                ad_name = row['ad_name']
                spend = row['spend']
                ctr = row['ctr']
                cpc = row['cpc']
                funnel_start_rate = row['funnel_start_rate']
                cpa = row['cpa']
                roas = row['roas']
                bookings = row['bookings']
                
                # Rule 2: After 24 hours (assuming spend > $100 means 24+ hours)
                if spend > 100:
                    if ctr <= 0.40 or cpc >= 6 or funnel_start_rate <= 15:
                        recommendations.append({
                            'ad_name': ad_name,
                            'action': 'pause',
                            'reason': f'Poor early performance: CTR {ctr:.2f}%, CPC ${cpc:.2f}, Funnel Start {funnel_start_rate:.1f}%',
                            'current_spend': spend,
                            'current_ctr': ctr,
                            'current_cpc': cpc,
                            'current_roas': roas,
                            'current_bookings': bookings,
                            'current_cpa': cpa
                        })
                        continue
                
                # Rule 3: Advanced performance (spend > $300)
                if spend > 300:
                    cvr = (bookings / row['site_visits'] * 100) if row['site_visits'] > 0 else 0
                    
                    if cvr > 2.5 and cpa < 120 and roas > 1:
                        recommendations.append({
                            'ad_name': ad_name,
                            'action': 'scale',
                            'reason': f'Excellent performance: CVR {cvr:.1f}%, CPA ${cpa:.2f}, ROAS {roas:.2f}',
                            'current_spend': spend,
                            'current_ctr': ctr,
                            'current_cpc': cpc,
                            'current_roas': roas,
                            'current_bookings': bookings,
                            'current_cpa': cpa
                        })
                    elif cvr <= 2.5 or cpa >= 120 or roas <= 1:
                        recommendations.append({
                            'ad_name': ad_name,
                            'action': 'pause',
                            'reason': f'Poor advanced performance: CVR {cvr:.1f}%, CPA ${cpa:.2f}, ROAS {roas:.2f}',
                            'current_spend': spend,
                            'current_ctr': ctr,
                            'current_cpc': cpc,
                            'current_roas': roas,
                            'current_bookings': bookings,
                            'current_cpa': cpa
                        })
                else:
                    # Monitor ads with less spend
                    recommendations.append({
                        'ad_name': ad_name,
                        'action': 'monitor',
                        'reason': f'Insufficient spend for decision: ${spend:.2f}',
                        'current_spend': spend,
                        'current_ctr': ctr,
                        'current_cpc': cpc,
                        'current_roas': roas,
                        'current_bookings': bookings,
                        'current_cpa': cpa
                    })
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating optimization recommendations: {e}")
            return []
    
    def generate_creative_brief(self, campaign_name, campaign_type, messaging_pillar, value_pillar, creative_format):
        """Generate AI-enhanced creative brief"""
        try:
            # Get performance insights
            performance_data = self.data.get('performance', [])
            successful_ads = [ad for ad in performance_data if ad.get('all_success', False)]
            
            # Prepare context for AI
            context = f"""
            Campaign: {campaign_name}
            Type: {campaign_type}
            Format: {creative_format}
            Messaging Pillar: {messaging_pillar}
            Value Pillar: {value_pillar}
            
            Performance Context:
            - Total ads analyzed: {len(performance_data)}
            - Successful ads: {len(successful_ads)}
            - Success rate: {len(successful_ads)/len(performance_data)*100:.1f}%
            """
            
            if successful_ads:
                # Add successful copy examples
                copy_examples = []
                for ad in successful_ads[:5]:  # Top 5 successful ads
                    if ad.get('primary_text') or ad.get('headline'):
                        copy_examples.append(f"Ad: {ad['ad_name']}\nHeadline: {ad.get('headline', 'N/A')}\nPrimary Text: {ad.get('primary_text', 'N/A')[:200]}...")
                
                if copy_examples:
                    context += f"\n\nSuccessful Copy Examples:\n" + "\n\n".join(copy_examples)
            
            messages = [
                {
                    "role": "system",
                    "content": "You are an expert Facebook media buyer and creative strategist. Generate comprehensive creative briefs based on performance data and winning patterns."
                },
                {
                    "role": "user",
                    "content": f"""
                    Create a comprehensive creative brief for a Facebook campaign with the following details:
                    
                    {context}
                    
                    Please include:
                    1. Campaign Overview (objectives, KPIs, flight dates)
                    2. Facebook Campaign Strategy (structure, targeting, budget)
                    3. Audience Insights (demographics, psychographics, motivations)
                    4. Messaging Framework (hooks, value props, CTAs specific to {messaging_pillar} and {value_pillar})
                    5. Visual Direction (format-specific guidelines for {creative_format})
                    6. Copy Recommendations (headlines and primary text based on successful patterns)
                    7. Success Metrics and Testing Plan
                    
                    Base recommendations on the successful ad patterns and copy examples provided.
                    """
                }
            ]
            
            ai_content = self.call_openai_api(messages, temperature=0.7, max_tokens=2000)
            
            return {
                'campaign_name': campaign_name,
                'campaign_type': campaign_type,
                'messaging_pillar': messaging_pillar,
                'value_pillar': value_pillar,
                'creative_format': creative_format,
                'ai_content': ai_content
            }
            
        except Exception as e:
            logger.error(f"Error generating creative brief: {e}")
            return {'error': str(e)}
    
    def generate_ai_insights(self, insight_type):
        """Generate AI insights based on type"""
        try:
            performance_data = self.data.get('performance', [])
            
            if insight_type == 'cluster':
                return self.generate_cluster_analysis()
            elif insight_type == 'creative_copy':
                return self.generate_creative_copy_analysis()
            elif insight_type == 'cro':
                return self.generate_cro_analysis()
            elif insight_type == 'strategy':
                return self.generate_strategy_recommendations()
            else:
                return {'error': 'Invalid insight type'}
                
        except Exception as e:
            logger.error(f"Error generating AI insights: {e}")
            return {'error': str(e)}
    
    def generate_cluster_analysis(self):
        """Generate cluster analysis insights"""
        try:
            performance_data = self.data.get('performance', [])
            successful_ads = [ad for ad in performance_data if ad.get('all_success', False)]
            
            # Prepare data summary
            summary = f"""
            Performance Summary:
            - Total Ads: {len(performance_data)}
            - Successful Ads: {len(successful_ads)}
            - Success Rate: {len(successful_ads)/len(performance_data)*100:.1f}%
            - Average ROAS: {np.mean([ad['roas'] for ad in performance_data]):.2f}
            - Average CTR: {np.mean([ad['ctr'] for ad in performance_data]):.2f}%
            """
            
            messages = [
                {
                    "role": "system",
                    "content": "You are a data analyst specializing in Facebook advertising performance. Analyze patterns and provide actionable insights."
                },
                {
                    "role": "user",
                    "content": f"""
                    Analyze the following Facebook ad performance data and provide cluster analysis insights:
                    
                    {summary}
                    
                    Please provide:
                    1. Performance pattern analysis
                    2. Success factor identification
                    3. Audience segment insights
                    4. Optimization opportunities
                    5. Scaling recommendations
                    
                    Focus on actionable insights for media buying optimization.
                    """
                }
            ]
            
            insights = self.call_openai_api(messages, temperature=0.7, max_tokens=1500)
            return {'insights': insights}
            
        except Exception as e:
            return {'error': str(e)}
    
    def generate_creative_copy_analysis(self):
        """Generate creative copy analysis"""
        try:
            performance_data = self.data.get('performance', [])
            ads_with_copy = [ad for ad in performance_data if ad.get('primary_text') or ad.get('headline')]
            successful_copy = [ad for ad in ads_with_copy if ad.get('all_success', False)]
            
            # Prepare copy examples
            copy_analysis = "Top Performing Copy Examples:\n\n"
            for i, ad in enumerate(successful_copy[:10], 1):
                copy_analysis += f"{i}. {ad['ad_name']} (ROAS: {ad['roas']:.2f})\n"
                copy_analysis += f"   Headline: {ad.get('headline', 'N/A')}\n"
                copy_analysis += f"   Primary Text: {ad.get('primary_text', 'N/A')[:150]}...\n\n"
            
            messages = [
                {
                    "role": "system",
                    "content": "You are a copywriting expert specializing in Facebook ad creative analysis. Identify patterns in successful ad copy."
                },
                {
                    "role": "user",
                    "content": f"""
                    Analyze the following successful Facebook ad copy and provide insights:
                    
                    {copy_analysis}
                    
                    Please provide:
                    1. Common messaging themes and patterns
                    2. Effective headline structures
                    3. Primary text best practices
                    4. Emotional triggers and hooks
                    5. CTA recommendations
                    6. Copy optimization suggestions
                    
                    Focus on actionable copywriting insights for future campaigns.
                    """
                }
            ]
            
            insights = self.call_openai_api(messages, temperature=0.7, max_tokens=1500)
            return {'insights': insights}
            
        except Exception as e:
            return {'error': str(e)}
    
    def generate_cro_analysis(self):
        """Generate CRO and funnel analysis"""
        try:
            performance_data = self.data.get('performance', [])
            
            # Calculate funnel metrics
            avg_funnel_start = np.mean([ad['funnel_start_rate'] for ad in performance_data])
            avg_survey_completion = np.mean([ad['survey_completion_rate'] for ad in performance_data])
            avg_checkout_start = np.mean([ad['checkout_start_rate'] for ad in performance_data])
            avg_booking_conversion = np.mean([ad['booking_conversion_rate'] for ad in performance_data])
            
            funnel_analysis = f"""
            Funnel Performance Analysis:
            - Average Funnel Start Rate: {avg_funnel_start:.1f}%
            - Average Survey Completion Rate: {avg_survey_completion:.1f}%
            - Average Checkout Start Rate: {avg_checkout_start:.1f}%
            - Average Booking Conversion Rate: {avg_booking_conversion:.1f}%
            
            Top Performing Funnel Examples:
            """
            
            # Add top funnel performers
            top_funnel = sorted(performance_data, key=lambda x: x['booking_conversion_rate'], reverse=True)[:5]
            for ad in top_funnel:
                funnel_analysis += f"\n{ad['ad_name']}: {ad['booking_conversion_rate']:.1f}% booking conversion"
            
            messages = [
                {
                    "role": "system",
                    "content": "You are a conversion rate optimization expert. Analyze funnel performance and provide CRO recommendations."
                },
                {
                    "role": "user",
                    "content": f"""
                    Analyze the following funnel performance data and provide CRO insights:
                    
                    {funnel_analysis}
                    
                    Please provide:
                    1. Funnel bottleneck identification
                    2. Landing page optimization opportunities
                    3. User experience improvements
                    4. Conversion rate optimization strategies
                    5. A/B testing recommendations
                    6. Technical optimization suggestions
                    
                    Focus on actionable CRO recommendations to improve conversion rates.
                    """
                }
            ]
            
            insights = self.call_openai_api(messages, temperature=0.7, max_tokens=1500)
            return {'insights': insights}
            
        except Exception as e:
            return {'error': str(e)}
    
    def generate_strategy_recommendations(self):
        """Generate strategic recommendations"""
        try:
            performance_data = self.data.get('performance', [])
            
            # Calculate key metrics
            total_spend = sum(ad['spend'] for ad in performance_data)
            total_revenue = sum(ad['revenue'] for ad in performance_data)
            overall_roas = total_revenue / total_spend if total_spend > 0 else 0
            
            strategy_context = f"""
            Campaign Performance Overview:
            - Total Spend: ${total_spend:,.2f}
            - Total Revenue: ${total_revenue:,.2f}
            - Overall ROAS: {overall_roas:.2f}
            - Total Ads: {len(performance_data)}
            - Success Rate: {len([ad for ad in performance_data if ad.get('all_success', False)])/len(performance_data)*100:.1f}%
            """
            
            messages = [
                {
                    "role": "system",
                    "content": "You are a senior Facebook advertising strategist. Provide high-level strategic recommendations for campaign optimization."
                },
                {
                    "role": "user",
                    "content": f"""
                    Based on the following campaign performance data, provide strategic recommendations:
                    
                    {strategy_context}
                    
                    Please provide:
                    1. Overall campaign health assessment
                    2. Budget allocation recommendations
                    3. Scaling strategies for successful elements
                    4. Risk mitigation for underperforming areas
                    5. Long-term optimization roadmap
                    6. Competitive positioning suggestions
                    
                    Focus on strategic, high-level recommendations for campaign growth.
                    """
                }
            ]
            
            insights = self.call_openai_api(messages, temperature=0.7, max_tokens=1500)
            return {'insights': insights}
            
        except Exception as e:
            return {'error': str(e)}
    
    def start_automation(self):
        """Start automated scheduling"""
        try:
            # Schedule data refresh every 12 hours
            self.scheduler.add_job(
                func=self.load_data,
                trigger="interval",
                hours=12,
                id='data_refresh'
            )
            
            # Schedule daily AI analysis at 9 AM
            self.scheduler.add_job(
                func=self.generate_daily_insights,
                trigger="cron",
                hour=9,
                id='daily_analysis'
            )
            
            self.scheduler.start()
            logger.info("✅ Automated scheduler started")
            
        except Exception as e:
            logger.error(f"❌ Error starting automation: {e}")
    
    def generate_daily_insights(self):
        """Generate daily automated insights"""
        try:
            logger.info("🤖 Generating daily AI insights...")
            # This could trigger automated reports, alerts, etc.
        except Exception as e:
            logger.error(f"Error generating daily insights: {e}")

# Initialize the tool
tool = FacebookOptimizationTool()

# Routes
@app.route('/')
def index():
    return render_template('enhanced_dashboard.html')

@app.route('/api/performance-summary')
def performance_summary():
    return jsonify(tool.performance_summary)

@app.route('/api/performance-data')
def performance_data():
    return jsonify(tool.data.get('performance', []))

@app.route('/api/optimization-recommendations')
def optimization_recommendations():
    recommendations = tool.get_optimization_recommendations()
    return jsonify(recommendations)

@app.route('/api/creative-brief', methods=['POST'])
def creative_brief():
    data = request.json
    brief = tool.generate_creative_brief(
        data.get('campaign_name'),
        data.get('campaign_type'),
        data.get('messaging_pillar'),
        data.get('value_pillar'),
        data.get('creative_format')
    )
    return jsonify(brief)

@app.route('/api/ai-insights')
def ai_insights():
    insight_type = request.args.get('type', 'cluster')
    insights = tool.generate_ai_insights(insight_type)
    return jsonify(insights)

@app.route('/api/automation-status')
def automation_status():
    status = {
        'auto_refresh_enabled': tool.scheduler.running,
        'google_sheets_connected': tool.gc is not None,
        'facebook_api_connected': bool(tool.fb_access_token and tool.fb_ad_account_id),
        'openai_configured': bool(tool.openai_api_key),
        'last_refresh': tool.last_refresh.isoformat() if tool.last_refresh else None,
        'next_scheduled_refresh': 'Every 12 hours',
        'next_scheduled_analysis': 'Daily at 9:00 AM',
        'date_range': 'June 2025 onwards'
    }
    return jsonify(status)

@app.route('/api/refresh-data', methods=['POST'])
def refresh_data():
    try:
        tool.load_data()
        return jsonify({'status': 'success', 'message': 'Data refreshed successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)})

@app.route('/api/test-openai')
def test_openai():
    try:
        if not tool.openai_api_key:
            return jsonify({'status': 'error', 'message': 'OpenAI API key not configured'})
        
        test_response = tool.call_openai_api([
            {"role": "user", "content": "Say 'OpenAI connection working' if you can read this."}
        ])
        
        if "OpenAI connection working" in test_response:
            return jsonify({'status': 'success', 'message': 'OpenAI connection working'})
        else:
            return jsonify({'status': 'error', 'message': test_response})
            
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/api/test-facebook')
def test_facebook():
    try:
        if not tool.fb_access_token or not tool.fb_ad_account_id:
            return jsonify({'status': 'error', 'message': 'Facebook API not configured'})
        
        # Test API call
        url = f"https://graph.facebook.com/v18.0/{tool.fb_ad_account_id}"
        params = {
            'access_token': tool.fb_access_token,
            'fields': 'name,account_status'
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            return jsonify({
                'status': 'success', 
                'message': f'Facebook API working. Account: {data.get("name", "Unknown")}'
            })
        else:
            return jsonify({
                'status': 'error', 
                'message': f'Facebook API error: {response.status_code}'
            })
            
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

