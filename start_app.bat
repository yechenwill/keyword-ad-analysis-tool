@echo off
echo 🔍 Keyword Ad Analysis Tool - Startup
echo ==================================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Error: Python is not installed or not in PATH
    echo    Please install Python 3.8 or higher
    pause
    exit /b 1
)

REM Check if we're in the right directory
if not exist "streamlit_app.py" (
    echo ❌ Error: streamlit_app.py not found in current directory
    echo    Please run this script from the project directory
    pause
    exit /b 1
)

REM Check VPN connection and start app
echo 🔒 Checking VPN connection...
python test_vpn_connection.py
if errorlevel 1 (
    echo.
    echo ❌ VPN connection check failed!
    echo    Please connect to your company VPN and try again.
    pause
    exit /b 1
)

echo.
echo ✅ All checks passed! Starting Streamlit app...
echo    The app will open in your browser shortly.
echo    Press Ctrl+C to stop the app.
echo.

REM Start the Streamlit app
python -m streamlit run streamlit_app.py

pause 