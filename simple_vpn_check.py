#!/usr/bin/env python3
"""
Simple VPN Check Tool
A lightweight tool to check VPN connectivity without requiring system permissions
"""

import requests
import socket
import time
import json
import sys
from urllib.parse import urlparse

def check_basic_connectivity():
    """Check basic connectivity to the API server"""
    print("🔍 Checking basic connectivity...")
    
    try:
        host = "prod-ssp-engine-private.ric1.admarketplace.net"
        port = 80
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print("✅ Basic connectivity: SUCCESS")
            return True
        else:
            print(f"❌ Basic connectivity: FAILED (error code: {result})")
            return False
            
    except Exception as e:
        print(f"❌ Basic connectivity: ERROR - {e}")
        return False

def check_api_endpoint():
    """Check if the API endpoint is accessible"""
    print("🌐 Checking API endpoint...")
    
    try:
        url = (
            'http://prod-ssp-engine-private.ric1.admarketplace.net/isp'
            '?plid=cjqduwisj4&results-ta=100&qt=test&country-code=US'
            '&region-code=&form-factor=desktop&os-family=windows'
            '&v=2.0&out=json&diag=enabled&ctaid='
        )
        
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            print("✅ API endpoint: SUCCESS")
            return True
        else:
            print(f"❌ API endpoint: FAILED (status: {response.status_code})")
            return False
            
    except requests.exceptions.ConnectionError as e:
        print(f"❌ API endpoint: CONNECTION ERROR - {e}")
        return False
    except requests.exceptions.Timeout:
        print("❌ API endpoint: TIMEOUT")
        return False
    except Exception as e:
        print(f"❌ API endpoint: ERROR - {e}")
        return False

def check_sample_query():
    """Test a sample query to verify the API returns expected data"""
    print("📊 Testing sample query...")
    
    try:
        url = (
            'http://prod-ssp-engine-private.ric1.admarketplace.net/isp'
            '?plid=cjqduwisj4&results-ta=100&qt=laptop&country-code=US'
            '&region-code=&form-factor=desktop&os-family=windows'
            '&v=2.0&out=json&diag=enabled&ctaid='
        )
        
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            text_ads = data.get('text_ads', [])
            
            if text_ads:
                print(f"✅ Sample query: SUCCESS ({len(text_ads)} ads found)")
                return True
            else:
                print("⚠️ Sample query: NO DATA (API responded but no ads found)")
                return True  # Still consider this a success
        else:
            print(f"❌ Sample query: FAILED (status: {response.status_code})")
            return False
            
    except Exception as e:
        print(f"❌ Sample query: ERROR - {e}")
        return False

def check_network_info():
    """Get basic network information"""
    print("🌐 Checking network information...")
    
    try:
        # Get external IP
        response = requests.get('https://httpbin.org/ip', timeout=10)
        if response.status_code == 200:
            external_ip = response.json().get('origin', 'Unknown')
            print(f"   External IP: {external_ip}")
        
        # Get local IP
        import subprocess
        if sys.platform == "darwin":  # macOS
            result = subprocess.run(['ifconfig'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                lines = result.stdout.split('\n')
                for line in lines:
                    if 'inet ' in line and '127.0.0.1' not in line:
                        ip = line.split()[1]
                        print(f"   Local IP: {ip}")
                        break
        elif sys.platform == "win32":  # Windows
            result = subprocess.run(['ipconfig'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                lines = result.stdout.split('\n')
                for line in lines:
                    if 'IPv4' in line:
                        ip = line.split(':')[1].strip()
                        print(f"   Local IP: {ip}")
                        break
        else:  # Linux
            result = subprocess.run(['hostname', '-I'], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                ip = result.stdout.strip().split()[0]
                print(f"   Local IP: {ip}")
        
    except Exception as e:
        print(f"   Network info: ERROR - {e}")

def main():
    print("=" * 60)
    print("🔒 Simple VPN Connection Check")
    print("=" * 60)
    print()
    
    # Check network info first
    check_network_info()
    print()
    
    # Run connectivity tests
    tests = [
        ("Basic Connectivity", check_basic_connectivity),
        ("API Endpoint", check_api_endpoint),
        ("Sample Query", check_sample_query)
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
        print("   You can now use the Keyword Ad Analysis Tool.")
        sys.exit(0)
    else:
        print("⚠️ Some tests failed. Please check your VPN connection.")
        print("\nTroubleshooting steps:")
        print("1. Ensure you're connected to your company VPN")
        print("2. Check with your IT department if you need VPN credentials")
        print("3. Try refreshing your VPN connection")
        print("4. Run this test again after connecting to VPN")
        print("\nIf you continue to have issues, contact your IT support team.")
        sys.exit(1)

if __name__ == "__main__":
    main() 