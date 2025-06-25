#!/usr/bin/env python3
"""
Debug script to help identify VPN connectivity issues
"""

import socket
import requests
import time
import sys

def debug_connectivity():
    """Debug connectivity issues"""
    print("🔍 Debugging VPN connectivity...")
    print("=" * 50)
    
    # Test 1: Basic socket connection
    print("\n1. Testing basic socket connection...")
    try:
        host = "prod-ssp-engine-private.ric1.admarketplace.net"
        port = 80
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            print("✅ Socket connection: SUCCESS")
        else:
            print(f"❌ Socket connection: FAILED (error code: {result})")
            return False
    except Exception as e:
        print(f"❌ Socket connection: ERROR - {e}")
        return False
    
    # Test 2: HTTP request with different timeouts
    print("\n2. Testing HTTP requests with different timeouts...")
    timeouts = [5, 10, 15, 30]
    
    for timeout in timeouts:
        print(f"   Testing with {timeout}s timeout...")
        try:
            url = "http://prod-ssp-engine-private.ric1.admarketplace.net"
            response = requests.get(url, timeout=timeout)
            print(f"   ✅ {timeout}s timeout: SUCCESS (status: {response.status_code})")
            break
        except requests.exceptions.Timeout:
            print(f"   ❌ {timeout}s timeout: TIMEOUT")
        except requests.exceptions.ConnectionError as e:
            print(f"   ❌ {timeout}s timeout: CONNECTION ERROR - {e}")
            break
        except Exception as e:
            print(f"   ❌ {timeout}s timeout: ERROR - {e}")
            break
    
    # Test 3: API endpoint test
    print("\n3. Testing API endpoint...")
    try:
        api_url = (
            'http://prod-ssp-engine-private.ric1.admarketplace.net/isp'
            '?plid=cjqduwisj4&results-ta=100&qt=test&country-code=US'
            '&region-code=&form-factor=desktop&os-family=windows'
            '&v=2.0&out=json&diag=enabled&ctaid='
        )
        
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json'
        }
        
        response = requests.get(api_url, headers=headers, timeout=15)
        print(f"✅ API endpoint: SUCCESS (status: {response.status_code})")
        
        if response.status_code == 200:
            data = response.json()
            print(f"   Response keys: {list(data.keys())}")
            if 'text_ads' in data:
                print(f"   Text ads count: {len(data['text_ads'])}")
        
    except Exception as e:
        print(f"❌ API endpoint: ERROR - {e}")
        return False
    
    # Test 4: Environment check
    print("\n4. Environment information...")
    print(f"   Python version: {sys.version}")
    print(f"   Requests version: {requests.__version__}")
    
    # Test 5: Network information
    print("\n5. Network information...")
    try:
        import subprocess
        result = subprocess.run(['ping', '-c', '3', 'prod-ssp-engine-private.ric1.admarketplace.net'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            print("✅ Ping test: SUCCESS")
            lines = result.stdout.split('\n')
            for line in lines:
                if 'time=' in line:
                    print(f"   {line.strip()}")
        else:
            print("❌ Ping test: FAILED")
    except Exception as e:
        print(f"❌ Ping test: ERROR - {e}")
    
    print("\n" + "=" * 50)
    print("🎉 Debug complete!")
    return True

if __name__ == "__main__":
    debug_connectivity() 