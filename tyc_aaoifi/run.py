from .business_activity_screen import BAScreen

def run():
    ba_screen = BAScreen()
    tolerance = float(input('Input Tolerance: '))
    end_date=input('Input End Date (default latest available date): ')
    while True:
        secu_code=input("Input Security Code: ")
        ba_screen.screen(secu_code,end_date,tolerance)
