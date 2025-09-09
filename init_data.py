from models import db, RestaurantInfo

init_data = [
    {
        "res_name": "육전국밥",
        "address": "관악구 관악로163",
        "lat": 37.4798856,
        "lng": 126.9520564,
        "res_phone": None,
        "category": "한식",
        "price": 77500,
        "score": 0.0,
    },
    {
        "res_name": "코코미",
        "address": "서울시 관악구 관악로146",
        "lat": 37.4782855,
        "lng": 126.9525606,
        "res_phone": None,
        "category": "한식",
        "price": 79500,
        "score": 0.0,
    },
    {
        "res_name": "가마솥",
        "address": "관악로17길 13",
        "lat": 37.4806542,
        "lng": 126.9510103,
        "res_phone": None,
        "category": "한식",
        "price": 119000,
        "score": 0.0,
    },
    {
        "res_name": "갱커피",
        "address": "관악로13길 7",
        "lat": 37.4791784,
        "lng": 126.9518288,
        "res_phone": None,
        "category": "카페",
        "price": 38100,
        "score": 0.0,
    },
    {
        "res_name": "어메이징디",
        "address": "관악로 144",
        "lat": 37.4779433,
        "lng": 126.9526208,
        "res_phone": None,
        "category": "카페",
        "price": 54300,
        "score": 0.0,
    },
    {
        "res_name": "하이보 서울대입구점",
        "address": "관악구 관악로 134",
        "lat": 37.4772463,
        "lng": 126.9527241,
        "res_phone": None,
        "category": "한식",
        "price": 200000,
        "score": 0.0,
    },
    {
        "res_name": "미엘케이커리",
        "address": "관악로14길 71",
        "lat": 37.478441,
        "lng": 126.9565537,
        "res_phone": None,
        "category": "디저트",
        "price": 81000,
        "score": 0.0,
    },
    {
        "res_name": "금야면옥",
        "address": "행운1길 15",
        "lat": 37.4799037,
        "lng": 126.9581989,
        "res_phone": None,
        "category": "면류",
        "price": 45000,
        "score": 0.0,
    },
    {
        "res_name": "마인드멜드",
        "address": "관악로14길 22",
        "lat": 37.4790228,
        "lng": 126.9537832,
        "res_phone": None,
        "category": "카페",
        "price": 41300,
        "score": 0.0,
    },
    {
        "res_name": "안녕부산",
        "address": "남부순환로226길 31",
        "lat": 37.4792382,
        "lng": 126.9538056,
        "res_phone": None,
        "category": "한식",
        "price": 56000,
        "score": 0.0,
    },
]

def insert_initial_restaurants():
    for data in init_data:
        restaurant = RestaurantInfo(**data)
        db.session.add(restaurant)

    db.session.commit()
    print("✅ 식당 정보 초기화 완료")