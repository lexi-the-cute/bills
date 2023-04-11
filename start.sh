#python3 -m compileall .venv > /dev/null 2>&1  # Compile Everything In Venv
python3 -m compileall src > /dev/null 2>&1 # Compile This Project's Code
python3 src/usa/federal/congress/api/download.py  # Run This Project