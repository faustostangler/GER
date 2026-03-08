from playwright.sync_api import sync_playwright

def main():
    print("Starting playwright...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = browser.new_context()
        page = context.new_page()
        
        print("Navigating to https://gercon.procempa.com.br/gerconweb/ ...")
        try:
            page.goto("https://gercon.procempa.com.br/gerconweb/", wait_until="networkidle", timeout=15000)
            print(f"Done navigating. Final URL: {page.url}")
        except Exception as e:
            print(f"Navigation error or timeout: {e}")
            print(f"Current URL: {page.url}")
            
        print(f"Page Title: {page.title()}")
        
        inputs = page.locator("input")
        count = inputs.count()
        print(f"\nFound {count} input fields in DOM.")
        for i in range(count):
            loc = inputs.nth(i)
            print(f"Input {i}: name={loc.get_attribute('name')}, type={loc.get_attribute('type')}, id={loc.get_attribute('id')}")
            
        with open("login_dom.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        print("Saved current DOM to login_dom.html")
            
        browser.close()

if __name__ == "__main__":
    main()
