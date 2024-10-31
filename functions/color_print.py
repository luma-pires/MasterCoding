class ColorPrint:

    @staticmethod
    def red_print(text):
        print(f"\033[91m{text}\033[0m")

    @staticmethod
    def yellow_print(text):
        print(f"\033[93m{text}\033[0m")

    @staticmethod
    def blue_print(text):
        print(f"\033[94m{text}\033[0m")

    @staticmethod
    def green_print(text):
        print(f"\033[92m{text}\033[0m")
