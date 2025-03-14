import http.server
import socketserver
import urllib.parse
from web_crawler import query_data

PORT = 8125

# Simple request handler to serve the HTML page and handle form submission
class MyHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            # HTML Template with form input
            with open("index.html", "r") as file:
                html_content = file.read()

            self.wfile.write(html_content.encode('utf-8'))


        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        # Read the content of the POST request
        length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(length)

        # Parse the form data (query parameter)
        data = urllib.parse.parse_qs(post_data.decode('utf-8'))

        # Get the search query from the form
        query = data.get('query', [''])[0]

        # Call the search function (replace with your actual search module function)
        result = query_data(query)  # Replace with your search function
        # Respond with the result
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

        # Convert the result list into a formatted string of links
        result_html = "".join([f'<p><a href="{item}">{item}</a></p>' for item in result])

        self.wfile.write(f"""
            <html>
                <head><title>Search Results</title></head>
                <body>
                    <h1>Search Results</h1>
                    <p>Query: {query}</p>
                    <h2>Results:</h2>
                    {result_html}
                    <a href="/">Back to search</a>
                </body>
        </html>
        """.encode('utf-8'))


# Set up the server
with socketserver.TCPServer(("", PORT), MyHandler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()

