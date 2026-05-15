import pyodbc
import random
import os
import re
import unicodedata
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from unidecode import unidecode

fake = Faker('vi_VN')

# Tự sinh tên Việt đúng format "Họ Tên Đệm Tên", tách tên nam,nữ 
_HO = [
    'Nguyễn', 'Trần', 'Lê', 'Phạm', 'Hoàng', 'Huỳnh', 'Phan', 'Vũ', 'Võ', 'Đặng',
    'Bùi', 'Đỗ', 'Hồ', 'Ngô', 'Dương', 'Lý', 'Mai', 'Trịnh', 'Đinh', 'Tô',
]
_NAM = {
    'dem': ['Văn', 'Hữu', 'Đức', 'Minh', 'Quốc', 'Công', 'Bảo', 'Gia', 'Trung',
            'Tấn', 'Quang', 'Phú', 'Tiến', 'Thành', 'Phước', 'Anh', 'Xuân', 'Hải'],
    'ten': ['An', 'Bình', 'Dũng', 'Đạt', 'Giang', 'Hùng', 'Khoa', 'Khôi', 'Long',
            'Nam', 'Phong', 'Phúc', 'Quân', 'Sang', 'Toàn', 'Tuấn', 'Tùng', 'Việt'],
}
_NU = {
    'dem': ['Thị', 'Ngọc', 'Thu', 'Thúy', 'Thanh', 'Kim', 'Mỹ', 'Thùy', 'Bích',
            'Thảo', 'Diễm', 'Phương', 'Ánh', 'Khánh', 'Linh', 'Tú', 'Vân', 'Hà'],
    'ten': ['An', 'Chi', 'Dung', 'Giang', 'Hà', 'Hương', 'Lan', 'Linh', 'Mai',
            'Nga', 'Ngọc', 'Nhung', 'Thảo', 'Thư', 'Trâm', 'Trinh', 'Uyên', 'Yến',
            'Quỳnh', 'Trang', 'Phương', 'Tú', 'Vân'],
}

def vn_name(gender: str = None) -> tuple:
    #Sinh tên Việt đúng chuẩn. Trả về (full_name, gender)
    if gender is None:
        gender = random.choice(['M', 'F'])
    ho   = random.choice(_HO)
    pool = _NAM if gender == 'M' else _NU
    ten  = f"{random.choice(pool['dem'])} {random.choice(pool['ten'])}"
    return f"{ho} {ten}", gender


# Sinh email thực tế từ tên 
def name_to_email(full_name: str, uid: int, domain: str = "gmail.com") -> str:
    full_name = full_name.replace('Đ', 'D').replace('đ', 'd')
    
    nfkd = unicodedata.normalize("NFKD", full_name)
    ascii_name = nfkd.encode("ascii", "ignore").decode("ascii").lower()
    parts = ascii_name.split()          # ['trinh', 'thu', 'tram']
    ho    = parts[0]                    # trinh
    dem   = parts[1] if len(parts) > 2 else ""   # thu
    ten   = parts[-1]                   # tram
    birth = random.randint(1970, 2005)

    style = random.randint(1, 4) # ở đây chỉ tạo cấu trúc mail phổ biến
    if style == 1:
        # trinhtram2005@gmail.com  →  họ + tên + năm sinh
        local = f"{ho}{ten}{birth}"
    elif style == 2:
        # tttram2005@gmail.com  →  chữ cái đầu họ + chữ cái đầu tên đệm + tên + năm sinh
        local = f"{ho[0]}{dem[0]}{ten}{birth}"
    elif style == 3:
        # nguyenvantoan99@gmail.com  →  họ + tên đệm + tên + 2 số cuối năm
        local = f"{ho}{dem}{ten}{str(birth)[-2:]}"
    else:
        # nvt2005@gmail.com  →  viết tắt chữ đầu + năm sinh
        initials = ho[0] + (dem[0] if dem else "") + ten[0]
        local = f"{initials}{birth}"

    local = re.sub(r"[^a-z0-9]", "", local)   # chỉ giữ chữ và số, không dấu chấm
    return f"{local}@{domain}"


# Sinh địa chỉ Việt Nam chuẩn 
_DUONG_PREFIX = ["Đường", "Phố", "Hẻm", "Ngõ", "Ngách"]
_TEN_DUONG = [
    "Lê Lợi", "Nguyễn Huệ", "Trần Phú", "Lý Thường Kiệt", "Đinh Tiên Hoàng",
    "Nguyễn Trãi", "Hoàng Diệu", "Phan Chu Trinh", "Bà Triệu", "Hai Bà Trưng",
    "Ngô Quyền", "Trần Hưng Đạo", "Võ Thị Sáu", "Cách Mạng Tháng 8", "Pasteur",
    "Nam Kỳ Khởi Nghĩa", "Điện Biên Phủ", "Lê Duẩn", "Hùng Vương", "Lê Hồng Phong",
    "Trường Chinh", "Giải Phóng", "Kim Mã", "Láng Hạ", "Nguyễn Chí Thanh",
]
_DON_VI = [
    ("Phường", "Quận 1",       "TP. Hồ Chí Minh"),
    ("Phường", "Quận 3",       "TP. Hồ Chí Minh"),
    ("Phường", "Quận 7",       "TP. Hồ Chí Minh"),
    ("Phường", "Bình Thạnh",   "TP. Hồ Chí Minh"),
    ("Phường", "Tân Bình",     "TP. Hồ Chí Minh"),
    ("Phường", "Hoàn Kiếm",    "Hà Nội"),
    ("Phường", "Đống Đa",      "Hà Nội"),
    ("Phường", "Cầu Giấy",     "Hà Nội"),
    ("Phường", "Hai Bà Trưng", "Hà Nội"),
    ("Phường", "Thanh Xuân",   "Hà Nội"),
    ("Phường", "Hải Châu",     "Đà Nẵng"),
    ("Phường", "Thanh Khê",    "Đà Nẵng"),
    ("Phường", "Ninh Kiều",    "Cần Thơ"),
    ("Xã",     "Hóc Môn",      "TP. Hồ Chí Minh"),
    ("Xã",     "Củ Chi",       "TP. Hồ Chí Minh"),
    ("Phường", "Lê Chân",      "Hải Phòng"),
    ("Phường", "Ngô Quyền",    "Hải Phòng"),
]

def vn_address(max_len: int = 150) -> str:
    so_nha = random.randint(1, 999)
    duong  = f"{random.choice(_DUONG_PREFIX)} {random.choice(_TEN_DUONG)}"
    don_vi, quan_huyen, tinh = random.choice(_DON_VI)
    phuong_so = random.randint(1, 20)
    addr = f"{so_nha} {duong}, {don_vi} {phuong_so}, {quan_huyen}, {tinh}"
    return addr[:max_len]


# Kết nối với SQL Server Management System
SERVER = 'localhost'
DATABASE = 'Order_Inventory'

CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"Trusted_Connection=yes;"
)

N_SUPPLIERS     = 500
N_CUSTOMERS     = 10_000
N_PRODUCTS      = 600
N_ORDERS        = 200_000
N_ORDER_DETAILS = 1_000_000


def get_connection():
    return pyodbc.connect(CONN_STR)

def get_engine():
    conn_url = (
        "mssql+pyodbc://localhost/Order_Inventory"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&Trusted_Connection=yes"
    )
    return create_engine(conn_url)

def truncate_all(conn):
    cursor = conn.cursor()
    deletes = [
        "DELETE FROM OrderDetails",
        "DELETE FROM Orders",
        "DELETE FROM Inventory",
        "DELETE FROM Products",
        "DELETE FROM Customers",
        "DELETE FROM Suppliers",
    ]
    reseeds = [
        "DBCC CHECKIDENT ('OrderDetails', RESEED, 0)",
        "DBCC CHECKIDENT ('Orders', RESEED, 0)",
        "DBCC CHECKIDENT ('Inventory', RESEED, 0)",
        "DBCC CHECKIDENT ('Products', RESEED, 0)",
        "DBCC CHECKIDENT ('Customers', RESEED, 0)",
        "DBCC CHECKIDENT ('Suppliers', RESEED, 0)",
    ]
    for sql in deletes + reseeds:
        cursor.execute(sql)
    conn.commit()
    print("Truncated & reset identity all tables\n")

def insert_df(conn, table: str, df: pd.DataFrame, batch_size: int = 10_000):
    cursor = conn.cursor()
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["?"] * len(df.columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    data = df.values.tolist()
    for start in range(0, len(data), batch_size):
        cursor.executemany(sql, data[start:start + batch_size])
        conn.commit()
    print(f"Inserted {len(df)} rows -> {table}")


# GENERATING DATA
# 1. Suppliers
_LOAI_CT = [
    "Công ty Cổ phần", "Công ty TNHH", "Công ty TNHH MTV",
    "Công ty Hợp danh", "Tập Đoàn",
]
_TEN_CT = [
    "Phát Đạt", "Thành Công", "Tiến Bộ", "Minh Châu", "Đại Việt",
    "Hòa Bình", "Thịnh Vượng", "Toàn Cầu", "Bình Minh", "Kim Long",
    "Vạn Lợi", "Sơn Hà", "Ánh Dương", "Trường Thịnh", "Đông Nam",
]

def vn_company() -> str:
    loai = random.choice(_LOAI_CT)
    ten = random.choice(_TEN_CT)
    return f"{loai} {ten}"

def gen_suppliers(n: int) -> pd.DataFrame:
    return pd.DataFrame([{
        "SupplierID":   i,
        "SupplierName": vn_company(),
        "Phone":        fake.phone_number()[:20],
        "Address":      vn_address()[:200],
    } for i in range(1, n + 1)])


# 2. Customers  
def gen_customers(n: int) -> pd.DataFrame:
    import numpy as np
    names_genders = [vn_name() for _ in range(n)]
    return pd.DataFrame([{
        "CustomerID":   i,
        "CustomerName": name,
        "Phone":        fake.phone_number()[:20],
        "Email":        name_to_email(name, i),
        "Address":      vn_address()[:200],
    } for i, (name, _) in enumerate(names_genders, start=1)])


# 3. Products (cần supplier_ids)
CATEGORIES = ["Điện tử", "Thời trang", "Thực phẩm", "Nội thất", "Sách", "Thể thao"]

_PRODUCT_NAMES = {
    "Điện tử": [
        ("Điện thoại", ["Samsung Galaxy", "iPhone", "Xiaomi Redmi", "OPPO Reno", "Vivo V", "Realme C", "Nokia G"]),
        ("Laptop", ["Dell Inspiron", "HP Pavilion", "Asus VivoBook", "Lenovo Thinkpad", "Acer Aspire", "MacBook Air"]),
        ("Tai nghe", ["Sony WH", "JBL Tune", "Samsung Galaxy Buds", "Apple AirPods", "Anker Soundcore"]),
        ("Máy tính bảng", ["iPad", "Samsung Galaxy Tab", "Xiaomi Pad", "Lenovo Tab"]),
        ("Smartwatch", ["Apple Watch", "Samsung Galaxy Watch", "Garmin Forerunner", "Xiaomi Smart Band"]),
        ("Loa Bluetooth", ["JBL Charge", "Bose SoundLink", "Sony SRS", "Marshall Emberton"]),
        ("Máy ảnh", ["Canon EOS", "Sony Alpha", "Nikon D", "Fujifilm X"]),
        ("Màn hình", ["LG UltraWide", "Samsung Odyssey", "Dell UltraSharp", "Asus ProArt"]),
        ("Bàn phím cơ", ["Keychron K", "Logitech MX Keys", "Corsair K", "Razer BlackWidow"]),
        ("Chuột gaming", ["Logitech G", "Razer DeathAdder", "SteelSeries Rival", "Zowie EC"]),
    ],
    "Thời trang": [
        ("Áo thun", ["Uniqlo", "Routine", "Owen", "Canifa", "Jules"]),
        ("Quần jean", ["Levi's", "Wrangler", "G-Star Raw", "Routine", "MOA"]),
        ("Áo khoác", ["The North Face", "Columbia", "Adidas", "Nike", "Puma"]),
        ("Váy", ["Zara", "H&M", "Mango", "Ivy Moda", "Elise"]),
        ("Giày sneaker", ["Nike Air Max", "Adidas Stan Smith", "New Balance 574", "Converse Chuck Taylor", "Vans Old Skool"]),
        ("Giày cao gót", ["Nine West", "Steve Madden", "Aldo", "Charles & Keith"]),
        ("Túi xách", ["Michael Kors", "Chanel", "Tumi", "Kipling", "Furla"]),
        ("Mũ", ["New Era", "Dickies", "Vans", "MLB", "Adidas"]),
        ("Đồng hồ thời trang", ["Daniel Wellington", "MVMT", "Fossil", "Seiko Presage"]),
        ("Kính mắt", ["Ray-Ban", "Oakley", "Gucci", "Gentle Monster"]),
    ],
    "Thực phẩm": [
        ("Cà phê", ["Highlands Coffee", "Trung Nguyên Legend", "G7", "Nescafé", "Vinacafé"]),
        ("Trà", ["Lipton", "Dilmah", "Trà Thái Nguyên", "Cozy", "Phúc Long"]),
        ("Mì ăn liền", ["Hảo Hảo", "Kokomi", "Omachi", "3 Miền", "Vifon", "Indomie"]),
        ("Bánh kẹo", ["Kinh Đô", "Bibica", "Oreo", "Pocky", "Choco Pie"]),
        ("Dầu ăn", ["Neptune", "Tường An", "Simply", "Meizan"]),
        ("Nước mắm", ["Chin-su", "Phú Quốc", "Nam Ngư"]),
        ("Sữa", ["TH True Milk", "Vinamilk", "Dutch Lady", "Mộc Châu"]),
        ("Gạo", ["Gạo ST25", "Jasmine", "Nàng Hoa", "Bắc Hương"]),
        ("Gia vị", ["Maggi", "Knorr", "Ajinomoto", "Cholimex"]),
        ("Snack", ["Oishi", "Poca", "Doritos", "Lays"]),
    ],
    "Nội thất": [
        ("Sofa", ["IKEA KIVIK", "Hòa Phát", "Nội thất Xanh", "JYSK", "Ashley"]),
        ("Bàn làm việc", ["IKEA LINNMON", "Hòa Phát", "Herman Miller", "Flexispot"]),
        ("Ghế văn phòng", ["Herman Miller Aeron", "Secretlab", "Ergohuman", "Hòa Phát HCG"]),
        ("Giường ngủ", ["IKEA MALM", "Kim Tiền", "Nội thất An Lành", "JYSK Maribo"]),
        ("Tủ quần áo", ["IKEA PAX", "Hòa Phát", "Xuân Hòa", "JYSK"]),
        ("Kệ sách", ["IKEA KALLAX", "Hòa Phát", "Nội thất Duy Phát"]),
        ("Đèn bàn", ["Philips", "IKEA FORSÅ", "Baseus", "Xiaomi MIJIA"]),
        ("Gương", ["IKEA HEMNES", "La Redoute", "Nội thất Việt"]),
        ("Thảm trải sàn", ["IKEA ÅDUM", "Carpet One", "Hòa Bình"]),
        ("Rèm cửa", ["IKEA MERETE", "JYSK", "Vải Thành Công"]),
    ],
    "Sách": [
        ("Tiểu thuyết", ["Nhà Giả Kim", "Đắc Nhân Tâm", "Sapiens", "Tôi Thấy Hoa Vàng Trên Cỏ Xanh", "Mắt Biếc"]),
        ("Sách kỹ năng", ["7 Thói Quen Hiệu Quả", "Mindset", "Atomic Habits", "Dám Bị Ghét", "Ikigai"]),
        ("Sách kinh tế", ["Cha Giàu Cha Nghèo", "Nghĩ Giàu Làm Giàu", "Đầu Tư Thông Minh"]),
        ("Sách lập trình", ["Clean Code", "Python Crash Course", "You Don't Know JS", "Eloquent JavaScript"]),
        ("Sách thiếu nhi", ["Doraemon", "Shin Cậu Bé Bút Chì", "Harry Potter", "Cô Bé Quàng Khăn Đỏ"]),
        ("Sách lịch sử", ["Việt Nam Sử Lược", "Lịch Sử Thế Giới", "Sapiens", "Homo Deus"]),
        ("Sách tâm lý", ["Tâm Lý Học Đám Đông", "Nghệ Thuật Tinh Tế", "Người Đàn Ông Tìm Kiếm Ý Nghĩa"]),
        ("Từ điển", ["Oxford Advanced", "Longman Dictionary", "Lạc Việt Anh-Việt"]),
        ("Sách giáo khoa", ["Toán 12", "Vật Lý 11", "Hóa Học 10", "Ngữ Văn 9", "Tiếng Anh 12"]),
        ("Sách nấu ăn", ["Bếp Của Mẹ", "Món Ngon Mỗi Ngày", "Ẩm Thực 3 Miền"]),
    ],
    "Thể thao": [
        ("Giày chạy bộ", ["Nike Air Zoom Pegasus", "Adidas Ultraboost", "Asics Gel-Kayano", "Brooks Ghost"]),
        ("Bóng đá", ["Adidas Tango", "Nike Flight", "Mikasa", "Molten"]),
        ("Vợt cầu lông", ["Yonex Astrox", "Victor Thruster", "Li-Ning Turbo", "Kawasaki"]),
        ("Vợt tennis", ["Wilson Blade", "Babolat Pure Drive", "Head Gravity", "Yonex EZONE"]),
        ("Dây nhảy", ["Jumping Pro", "Adidas", "Nike", "Everlast"]),
        ("Găng tay boxing", ["Everlast", "Hayabusa", "Fairtex", "Twins"]),
        ("Xe đạp", ["Giant ATX", "Trek Marlin", "Specialized Rockhopper", "Asama"]),
        ("Dụng cụ yoga", ["Manduka PRO", "Liforme", "Gaiam", "Adidas"]),
        ("Tạ tay", ["Bowflex", "Xiaomi", "PowerBlock", "CAP Barbell"]),
        ("Áo thể thao", ["Nike Dri-FIT", "Adidas Climacool", "Under Armour", "Puma Dry Cell"]),
    ],
}

_SUFFIX = {
    "Điện tử":    ["Series {}", "Pro {}", "Ultra {}", "Plus {}", "{}i", "{}X"],
    "Thời trang": ["size S", "size M", "size L", "size XL", "màu đen", "màu trắng", "màu navy"],
    "Thực phẩm":  ["500g", "1kg", "250ml", "500ml", "1 lít", "hộp 24 gói", "túi 2kg", "thùng 24 lon", "200g", "lốc 4 hộp"],
    "Nội thất":   ["màu walnut", "màu trắng sữa", "màu oak", "màu đen mờ", "bộ 2 cái", "bộ 4 cái"],
    "Sách":       ["(Bìa Cứng)", "(Tái Bản)", "(Bìa Mềm)", "- Ấn Bản Đặc Biệt", "Tập 1", "Tập 2", "Tập 3"],
    "Thể thao":   ["size 38", "size 40", "size 42", "size 44", "màu đen/đỏ", "màu xanh/trắng", "màu đen/vàng"],
}

def _random_suffix(category: str) -> str:
    tpl = random.choice(_SUFFIX[category])
    if '{}' in tpl:
        parts = tpl.split('{}')
        filled = parts[0]
        for p in parts[1:]:
            num = random.choice([8, 16, 32, 64, 100, 200, 250, 500])
            filled += str(num) + p
        return filled
    return tpl

def vn_product_name(category: str) -> str:
    type_name, brands = random.choice(_PRODUCT_NAMES[category])
    brand = random.choice(brands)
    suffix = _random_suffix(category)
    return f"{type_name} {brand} {suffix}"[:85]

# Tạo giá phù hợp cho từng phân khúc sản phẩm
_PRICE_RANGE = {
    "Điện tử": {
        "Điện thoại":    (2_000_000,  35_000_000),
        "Laptop":        (8_000_000,  60_000_000),
        "Tai nghe":        (200_000,   5_000_000),
        "Máy tính bảng": (3_000_000,  25_000_000),
        "Smartwatch":    (1_000_000,  15_000_000),
        "Loa Bluetooth":   (300_000,   5_000_000),
        "Máy ảnh":       (5_000_000,  60_000_000),
        "Màn hình":      (2_000_000,  20_000_000),
        "Bàn phím cơ":     (500_000,   5_000_000),
        "Chuột gaming":    (200_000,   3_000_000),
    },
    "Thời trang": {
        "Áo thun":           (100_000,   800_000),
        "Quần jean":         (200_000, 2_000_000),
        "Áo khoác":          (300_000, 3_000_000),
        "Váy":               (150_000, 1_500_000),
        "Giày sneaker":      (300_000, 4_000_000),
        "Giày cao gót":      (250_000, 3_000_000),
        "Túi xách":          (200_000,20_000_000),
        "Mũ":                 (80_000,   500_000),
        "Đồng hồ thời trang":(500_000, 8_000_000),
        "Kính mắt":          (200_000, 5_000_000),
    },
    "Thực phẩm": {
        "Cà phê":    (50_000,   500_000),
        "Trà":       (30_000,   300_000),
        "Mì ăn liền":(5_000,    80_000),
        "Bánh kẹo":  (15_000,   200_000),
        "Dầu ăn":    (30_000,   150_000),
        "Nước mắm":  (20_000,   120_000),
        "Sữa":       (25_000,   500_000),
        "Gạo":       (20_000,   200_000),
        "Gia vị":    (10_000,   100_000),
        "Snack":     (10_000,    80_000),
    },
    "Nội thất": {
        "Sofa":           (3_000_000, 30_000_000),
        "Bàn làm việc":   (1_000_000, 15_000_000),
        "Ghế văn phòng":  (1_500_000, 25_000_000),
        "Giường ngủ":     (2_000_000, 20_000_000),
        "Tủ quần áo":     (1_500_000, 15_000_000),
        "Kệ sách":          (500_000,  5_000_000),
        "Đèn bàn":          (150_000,  1_500_000),
        "Gương":            (200_000,  3_000_000),
        "Thảm trải sàn":    (300_000,  5_000_000),
        "Rèm cửa":          (200_000,  3_000_000),
    },
    "Sách": {
        "Tiểu thuyết":   (50_000,  300_000),
        "Sách kỹ năng":  (60_000,  300_000),
        "Sách kinh tế":  (60_000,  280_000),
        "Sách lập trình":(80_000,  400_000),
        "Sách thiếu nhi":(30_000,  150_000),
        "Sách lịch sử":  (50_000,  250_000),
        "Sách tâm lý":   (55_000,  270_000),
        "Từ điển":      (100_000,  500_000),
        "Sách giáo khoa":(25_000,  120_000),
        "Sách nấu ăn":   (60_000,  250_000),
    },
    "Thể thao": {
        "Giày chạy bộ":    (800_000,  5_000_000),
        "Bóng đá":         (100_000,    800_000),
        "Vợt cầu lông":    (300_000,  5_000_000),
        "Vợt tennis":      (500_000,  8_000_000),
        "Dây nhảy":         (50_000,    300_000),
        "Găng tay boxing": (200_000,  2_000_000),
        "Xe đạp":        (2_000_000, 25_000_000),
        "Dụng cụ yoga":    (150_000,  1_500_000),
        "Tạ tay":          (100_000,  3_000_000),
        "Áo thể thao":     (150_000,  1_000_000),
    },
}

def _price_for(category: str, product_name: str) -> float:
    ranges = _PRICE_RANGE.get(category, {})
    for type_name, (lo, hi) in ranges.items():
        if product_name.startswith(type_name):
            return int(round(random.uniform(lo, hi), -3))
    # fallback: lấy min/max của toàn danh mục
    if ranges:
        lo = min(v[0] for v in ranges.values())
        hi = max(v[1] for v in ranges.values())
        return int(round(random.uniform(lo, hi), -3))
    return int(round(random.uniform(100_000, 5_000_000), -3))

def gen_products(n: int, supplier_ids: list) -> pd.DataFrame:
    import numpy as np
    cats  = [random.choice(CATEGORIES) for _ in range(n)]
    names = [vn_product_name(c) for c in cats]
    return pd.DataFrame({
        "ProductID":   np.arange(1, n + 1),
        "ProductName": names,
        "SupplierID":  np.random.choice(supplier_ids, size=n),
        "Price":       [_price_for(c, nm) for c, nm in zip(cats, names)],
        "Category":    cats,
    })


# 4. Inventory (cần product_ids)
def gen_inventory(n: int, product_ids: list) -> pd.DataFrame:
    import numpy as np
    return pd.DataFrame({
        "InventoryID":     np.arange(1, n + 1),
        "ProductID":       product_ids,
        "QuantityInStock": np.random.randint(0, 501, size=n),
        "LastUpdated": [
            fake.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
            for _ in range(n)
        ],
    })


# 5. Orders (cần customer_ids)
def gen_order(n: int, customer_ids: list) -> pd.DataFrame:
    import numpy as np
    start = datetime(2023, 1, 1)
    offsets = np.random.randint(0, 730, size=n)
    dates   = [(start + timedelta(days=int(d))).strftime("%Y-%m-%d") for d in offsets]
    return pd.DataFrame({
        "OrderID":     np.arange(1, n + 1),
        "CustomerID":  np.random.choice(customer_ids, size=n),
        "OrderDate":   dates,
        "TotalAmount": 0,
    })


# 6. OrderDetails (cần product_ids, order_ids, products dataframe để lấy giá)
def gen_order_details(n: int, order_ids: list, products_df: pd.DataFrame):
    import numpy as np

    product_ids  = products_df["ProductID"].to_numpy()
    prices       = products_df["Price"].to_numpy(dtype=float)

    # Vectorized random sampling
    rand_product_idx = np.random.randint(0, len(product_ids), size=n)
    rand_order_idx   = np.random.randint(0, len(order_ids),   size=n)
    quantities       = np.random.randint(1, 21,               size=n)

    chosen_products = product_ids[rand_product_idx]
    chosen_orders   = np.array(order_ids)[rand_order_idx]
    chosen_prices   = prices[rand_product_idx]
    total_prices    = np.round(chosen_prices * quantities, 0).astype(int)

    return pd.DataFrame({
        "OrderDetailID": np.arange(1, n + 1),
        "OrderID":       chosen_orders,
        "ProductID":     chosen_products,
        "Quantity":      quantities,
        "Price":         total_prices,
    })


# Update TotalAmount in order
def update_total_amount(conn, order_details_df: pd.DataFrame):
    totals = order_details_df.groupby("OrderID")["Price"].sum().reset_index()
    cursor = conn.cursor()
    for _, row in totals.iterrows():
        cursor.execute(
            "UPDATE Orders SET TotalAmount = ? WHERE OrderID = ?",
            float(row["Price"]), int(row["OrderID"])
        )
    conn.commit()
    print(f"Updated TotalAmount for {len(totals)} orders")


# Tạo file CSV
def export_all(fmt: str = "csv"):
    engine = get_engine()
    tables = ["Suppliers", "Customers", "Products", "Inventory", "Orders", "OrderDetails"]
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, "output")
    os.makedirs(output_dir, exist_ok=True)
    
    # Các cột tiền cần format dạng 1.000.000đ
    price_cols = {
        "Products":     ["Price"],
        "Orders":       ["TotalAmount"],
        "OrderDetails": ["Price"],
    }

    def fmt_price(x):
        return f"{int(x):,}đ".replace(",", ".")

    for table in tables:
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        for col in price_cols.get(table, []):
            if col in df.columns:
                df[col] = df[col].apply(fmt_price)
        if fmt == "csv":
            df.to_csv(os.path.join(output_dir, f"{table}.csv"), index=False, encoding="utf-8-sig")
        else:
            df.to_json(os.path.join(output_dir, f"{table}.json"), orient="records", force_ascii=False, indent=2)
        print(f"Exported {table}.{fmt} ({len(df)} rows)")


# Chạy File
def main():
    print(f"\n[{datetime.now():%H:%M:%S}] Connecting SSMS...")
    conn = get_connection()
    print(f"Connect Successfully!\n")

    truncate_all(conn)

    print(f"Generating and inserting data...")

    suppliers = gen_suppliers(N_SUPPLIERS)
    customers = gen_customers(N_CUSTOMERS)
    
    insert_df(conn, "Suppliers", suppliers[["SupplierName", "Phone", "Address"]])

    cursor = conn.cursor()
    cursor.execute("SELECT SupplierID from Suppliers")
    real_supplier_ids = [row[0] for row in cursor.fetchall()]

    products  = gen_products(N_PRODUCTS, real_supplier_ids)
    inventory = gen_inventory(N_PRODUCTS, products["ProductID"].tolist())

    insert_df(conn, "Customers", customers[["CustomerName", "Phone", "Email", "Address"]])

    cursor.execute("SELECT CustomerID from Customers")
    real_customer_ids = [row[0] for row in cursor.fetchall()]

    order = gen_order(N_ORDERS, real_customer_ids)

    insert_df(conn, "Products", products[["ProductName", "SupplierID", "Price", "Category"]])

    cursor.execute("SELECT ProductID from Products")
    real_product_ids = [row[0] for row in cursor.fetchall()]

    inventory = gen_inventory(len(real_product_ids), real_product_ids)
    insert_df(conn, "Inventory", inventory[["ProductID", "QuantityInStock", "LastUpdated"]])
    insert_df(conn, "Orders", order[["CustomerID", "OrderDate", "TotalAmount"]])

    cursor.execute("SELECT OrderID FROM Orders")
    real_order_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT ProductID, Price FROM Products")
    rows = cursor.fetchall()
    real_products_df = pd.DataFrame(list(map(tuple, rows)), columns=["ProductID", "Price"])

    order_details = gen_order_details(N_ORDER_DETAILS, real_order_ids, real_products_df)
    insert_df(conn, "OrderDetails", order_details[["OrderID", "ProductID", "Quantity", "Price"]])

    update_total_amount(conn, order_details)

    export_all(fmt="csv")

    conn.close()
    print(f"\n[{datetime.now():%H:%M:%S}] Done.")


if __name__ == "__main__":
    main()