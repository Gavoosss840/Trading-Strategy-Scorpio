@echo off
title MM Options Trading Bot - Auto Restart
echo ========================================
echo MM Options Trading Bot (Auto-Restart)
echo ========================================
echo.

:loop
echo [%date% %time%] Starting bot...
python "c:\Users\hugob\OneDrive\Desktop\Professionnel\B. Horizon\Trading-algo\MM_Options_Trading_Bot.py"

echo.
echo [%date% %time%] Bot stopped. Restarting in 10 seconds...
echo Press Ctrl+C to stop permanently.
timeout /t 10

goto loop