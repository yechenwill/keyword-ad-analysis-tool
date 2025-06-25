#!/usr/bin/env python3
"""
VPN Connection Test Script
This script helps verify that your VPN connection is working before running the main app.
"""

import socket
import requests
import sys
import time

# API endpoint template (same as in main app)
API_URL_TEMPLATE = (
    'http://prod-ssp-engine-private.ric1.admarketplace.net/isp'
    '?plid=cjqduwisj4&results-ta=100&qt={}&country-code={}'
    '&region-code=&form-factor={}&os-family=windows'
    '&v=2.0&out=json&diag=enabled&ctaid='
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'application/json'
}

def test_basic_connectivity():
    """Test basic network connectivity to the API server"""
    print("🔍 Testing basic connectivity...")
    
    try:
        host = "prod-ssp-engine-private.ric1.admarketplace.net"
        port = 80
        
        # Try to connect to the server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print("✅ Basic connectivity: SUCCESS")
            return True
        else:
            print("❌ Basic connectivity: FAILED")
            print(f"   Error code: {result}")
            return False
            
    except Exception as e:
        print(f"❌ Basic connectivity: ERROR - {str(e)}")
        return False

def test_api_endpoint():
    """Test the actual API endpoint"""
    print("🌐 Testing API endpoint...")
    
    try:
        # Use a simple test query
        test_url = API_URL_TEMPLATE.format("test", "US", "desktop")
        print(f"   Testing URL: {test_url}")
        
        response = requests.get(test_url, headers=HEADERS, timeout=10)
        
        if response.status_code == 200:
            print("✅ API endpoint: SUCCESS")
            print(f"   Status code: {response.status_code}")
            return True
        else:
            print(f"❌ API endpoint: FAILED")
            print(f"   Status code: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ API endpoint: CONNECTION ERROR")
        print("   Please check your VPN connection")
        return False
    except requests.exceptions.Timeout:
        print("❌ API endpoint: TIMEOUT")
        print("   Please check your VPN connection")
        return False
    except Exception as e:
        print(f"❌ API endpoint: ERROR - {str(e)}")
        return False

def test_sample_query():
    """Test a sample query to verify the API returns expected data"""
    print("📊 Testing sample query...")
    
    try:
        # Test with a simple keyword
        test_url = API_URL_TEMPLATE.format("laptop", "US", "desktop")
        response = requests.get(test_url, headers=HEADERS, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            text_ads = data.get('text_ads', [])
            
            if text_ads:
                print(f"✅ Sample query: SUCCESS")
                print(f"   Found {len(text_ads)} ads")
                print(f"   Sample advertiser: {text_ads[0].get('adv_name', 'N/A')}")
                return True
            else:
                print("⚠️ Sample query: NO DATA")
                print("   API responded but no ads found")
                return True  # Still consider this a success
        else:
            print(f"❌ Sample query: FAILED")
            print(f"   Status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Sample query: ERROR - {str(e)}")
        return False

def main():
    print("=" * 60)
    print("🔒 VPN Connection Test Tool")
    print("=" * 60)
    print()
    
    # Run all tests
    tests = [
        ("Basic Connectivity", test_basic_connectivity),
        ("API Endpoint", test_api_endpoint),
        ("Sample Query", test_sample_query)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 40)
        result = test_func()
        results.append((test_name, result))
        time.sleep(1)  # Small delay between tests
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
        if not result:
            all_passed = False
    
    print()
    if all_passed:
        print("🎉 All tests passed! Your VPN connection is working correctly.")
        print("   You can now run the main Streamlit app.")
        sys.exit(0)
    else:
        print("⚠️ Some tests failed. Please check your VPN connection.")
        print("\nTroubleshooting steps:")
        print("1. Ensure you're connected to your company VPN")
        print("2. Check with your IT department if you need VPN credentials")
        print("3. Try refreshing your VPN connection")
        print("4. Run this test again after connecting to VPN")
        sys.exit(1)

if __name__ == "__main__":
    main() 