from bottle import route, run, request
import subprocess

@route('/write_to_file')
def write_to_file():
    date_param = request.query.date
    command = ['python', 'demo.py', date_param]
    
    # 使用subprocess模块执行Python脚本a.py，并将参数值写入a.txt文件
    # http://localhost:8080/write_to_file?date=your_date_here
    subprocess.run(command)
    
    return "Parameter value written to a.txt"

if __name__ == '__main__':
    run(host='localhost', port=8080)