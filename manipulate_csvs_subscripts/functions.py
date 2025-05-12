def merge_rows(group):
    """
    Merges multiple rows within a group by keeping the first non-null value for each column.

    Args:
        group (pd.DataFrame): Grouped DataFrame.

    Returns:
        pd.Series: A single merged row.
    """
    return group.apply(lambda col: col.dropna().iloc[0] if not col.dropna().empty else None)


def extract_month_year(text):
    if isinstance(text, str):
        words = text.split()
        if words[:2] == ["Not", "applicable"]:
            return "9999-01-01"  # Set default date if "Not applicable"
        elif len(words) >= 2:
            month_year_str = f"01 {' '.join(words[-2:])}"  # Prefix "01" to last two words
            try:
                # Convert to datetime object and format as YYYY-MM-DD
                date_obj = datetime.strptime(month_year_str, "%d %B %Y")
                return date_obj.strftime("%Y-%m-%d")  
            except ValueError:
                print(f"⚠️ Invalid date format: {month_year_str}")  # Debugging for unexpected cases
                return None  # Return None for invalid dates
    return None  # Return None if condition not met

def extract_status(text):
    if isinstance(text, str):
        words = text.split()
        if words[:2] == ["Not", "applicable"]:  
            return "Not applicable"  # Return as-is
        elif len(words) > 2:
            return " ".join(words[:-2])  # Get everything except the last two words
    return None  # Return None if there are 2 or fewer words and not "Not applicable"

def extract_month_year_from_group(text):
    if isinstance(text, str):
        words = text.split()
        if words[:2] == ["Date", "not"]:
            return "9999-01-01"  # Set default date if "Date not determined"
        else:
            return words[1] + '-' + words[0] + "-01"