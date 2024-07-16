import subprocess
from django.shortcuts import render
import csv
import io  

def button(request):
    print('Rendering initial page')
    return render(request, 'home.html')

def run_script(request):
    print('Running the Python script')

    python_path = 'python'  # or 'python3' depending on your setup
    script_path_1 = r'C:\\Users\\Naxtre\\OneDrive\\Desktop\\New folder\\ALL_ROUND_DATA\\ALL_STATE_DATA_BY_BC_ROUND\\BC_Round_data_calculation.py'

    table_data = []  # Initialize table_data here
    try:
        result = subprocess.run([python_path, script_path_1], check=True, capture_output=True, text=True)
        script_output = result.stdout.strip()  # Remove leading/trailing whitespace
    except subprocess.CalledProcessError as e:
        print('Error running script:', e)
        script_output = f"Script error: {e}"  # Provide user-friendly error message

    if script_output:
        for line in script_output.splitlines():
            table_data.append(line.split())

        request.session['script_output'] = script_output
        request.session['table_data'] = table_data

    context = {'table_data': table_data}  # Create context for template access
    return render(request, 'home.html', context)

def dbinsert(request):
    print('Inserting the data in SQL Server')

    python_path = 'python'  # or 'python3' depending on your setup
    script_path_2 = r'C:\\Users\\Naxtre\\OneDrive\\Desktop\\New folder\\ALL_ROUND_DATA\\database\\insert_dataframe.py'

    script_output = request.session.get('script_output', 'No output from previous script.')

    try:
        result = subprocess.run([python_path, script_path_2], check=True, capture_output=False, text=False)
        script_insert = result.stdout.strip()  # Remove leading/trailing whitespace
    except subprocess.CalledProcessError as e:
        print('Error running script:', e)
        script_insert = f"Script error: {e}"

    context = {'script_insert': script_insert}
    return render(request, 'home.html', context)
