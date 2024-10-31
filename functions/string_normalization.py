from unidecode import unidecode


class StringNormalization:

    @staticmethod
    def treatment_strings(string):

        if isinstance(string, str):

            # Quote problems:
            string = string.replace("'", "")
            # Special strings:
            string = unidecode(string)
            # Lower case:
            string = string.lower()

        return string
