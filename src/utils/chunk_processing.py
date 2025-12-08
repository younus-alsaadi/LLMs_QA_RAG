def clean_text_for_db(text: str) -> str:
    if text is None:
        return text
    # remove null bytes
    text = text.replace("\x00", "")
    #  strip other weird control chars
    text = "".join(ch for ch in text if ch >= " " or ch in "\n\r\t")
    return text
