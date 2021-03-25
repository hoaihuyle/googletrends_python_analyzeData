# googletrends_python_analyzeData
A console tool connect the google trends API (https://trends.google.com/trends/trendingsearches/daily?geo=US) to store data into PostgreSQL and analyze the data by export into excel and chart

DATABASE
Thực hiện tạo bảng tên vn_trending theo cấu trúc các trường như sau:

1.	Id -	Sinh tự động giá trị cho trường này. Đây là trường lưu giá trị dạng số
2.	keyword -	kiểu ký tự, lưu keyword tìm kiếm
3.	date -	kiểu date, lưu ngày tìm kiếm của keyword
4.	Value -	Kiểu số, lưu số lần tìm kiếm trong ngày của keyword
5.	trend_type -	lưu trữ các nhóm tin tức: news, film, songs,... trong file key_trends.xls
