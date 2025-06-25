#!/usr/bin/env python3
"""
Startup script for the Keyword Ad Analysis Tool
This script checks VPN connectivity before launching the Streamlit app.
"""

import subprocess
import sys
import os
from test_vpn_connection import test_basic_connectivity, test_api_endpoint

def main():
    print("🔍 Keyword Ad Analysis Tool - Startup")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not os.path.exists("streamlit_app.py"):
        print("❌ Error: streamlit_app.py not found in current directory")
        print("   Please run this script from the project directory")
        sys.exit(1)
    
    # Check VPN connectivity
    print("\n🔒 Checking VPN connection...")
    
    vpn_ok = test_basic_connectivity()
    if not vpn_ok:
        print("\n❌ VPN connection check failed!")
        print("   Please connect to your company VPN and try again.")
        print("   Run 'python test_vpn_connection.py' for detailed diagnostics.")
        sys.exit(1)
    
    # Check API endpoint
    print("\n🌐 Checking API endpoint...")
    api_ok = test_api_endpoint()
    if not api_ok:
        print("\n❌ API endpoint check failed!")
        print("   Please check your VPN connection and try again.")
        print("   Run 'python test_vpn_connection.py' for detailed diagnostics.")
        sys.exit(1)
    
    print("\n✅ All checks passed! Starting Streamlit app...")
    print("   The app will open in your browser shortly.")
    print("   Press Ctrl+C to stop the app.")
    
    # Launch Streamlit app
    try:
        subprocess.run([sys.executable, "-m", "streamlit", "run", "streamlit_app.py"])
    except KeyboardInterrupt:
        print("\n👋 App stopped by user")
    except Exception as e:
        print(f"\n❌ Error starting app: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 