class Parser:

    @staticmethod
    def review_fetch(json: dict, is_widget: bool):
        page = json['response']['reviews'] if is_widget else json['reviews']