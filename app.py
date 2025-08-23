            
            # Debug: Show specific ad set data
            for ad in combined_data:
                if '071425_CEO_AppointmentPage_Calgary_Amazon_130_EngagedShoppers_Video_Feed-Stories-Reels_EXP-InsuranceLP' in ad['ad_set_name']:
                    print(f"📊 Processed target ad set: {ad['ad_set_name']}")
                    print(f"   - Final CTR: {ad['ctr']:.2f}%")
                    print(f"   - Final Impressions: {ad['impressions']:,.0f}")
                    print(f"   - Final Clicks: {ad['clicks']:,.0f}")
                    print(f"   - Final CPC: ${ad['cpc']:.2f}")
                    print(f"   - Final CPM: ${ad['cpm']:.2f}")
                    print(f"   - Success Criteria: {ad['success_criteria']}")
                    break
            
            if processed_count == 0:
                print("⚠️ No records were successfully combined - check data format...")
                
        except Exception as e:
            print(f"❌ Data processing error: {e}")

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
                'avg_cpm': 0,
                'successful_ads': 0
            }
        
        total_spend = sum(ad['spend'] for ad in self.performance_data)
        total_revenue = sum(ad['revenue'] for ad in self.performance_data)
        total_clicks = sum(ad['clicks'] for ad in self.performance_data)
        total_impressions = sum(ad['impressions'] for ad in self.performance_data)
        total_bookings = sum(ad['bookings'] for ad in self.performance_data)
        
        avg_ctr = sum(ad['ctr'] for ad in self.performance_data) / len(self.performance_data)
        avg_cpc = sum(ad['cpc'] for ad in self.performance_data if ad['cpc'] > 0) / max(1, len([ad for ad in self.performance_data if ad['cpc'] > 0]))
        avg_cpm = sum(ad['cpm'] for ad in self.performance_data if ad['cpm'] > 0) / max(1, len([ad for ad in self.performance_data if ad['cpm'] > 0]))
        avg_cpa = sum(ad['cpa'] for ad in self.performance_data if ad['cpa'] > 0) / max(1, len([ad for ad in self.performance_data if ad['cpa'] > 0]))
        avg_roas = sum(ad['roas'] for ad in self.performance_data if ad['roas'] > 0) / max(1, len([ad for ad in self.performance_data if ad['roas'] > 0]))
        
        successful_ads = len([ad for ad in self.performance_data if ad['all_criteria_met']])
