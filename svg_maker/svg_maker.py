import svgwrite

def create_webfont_font_template(file_name, width, height):
    dwg = svgwrite.Drawing(file_name, (750, 750), debug=True)
    # font data downloaded from google fonts
    dwg.embed_google_web_font(name="Indie Flower", uri='http://fonts.googleapis.com/css?family=Indie+Flower')
    dwg.embed_stylesheet("""
    .fontyfont {{
        font-family: "{font_name}";
        font-size: {font_size};
    }}
    """)
    # This should work stand alone and embedded in a website!
    paragraph = dwg.add(dwg.g(class_="fontyfont", ))
    paragraph.add(dwg.text("MICHAEL", insert=(0, 100)))
    dwg.save(pretty=True)

def create_font_template(file_name, font_name, font_file, font_size, width, height):

    dwg = svgwrite.Drawing(file_name, (750, 750), debug=True)
    dwg.embed_font(name=font_name, filename=font_file)
    dwg.embed_stylesheet(f"""
    .fontyfont {{
        font-family: "{font_name}";
        font-size: {font_size};
    }}
    """)
    # This should work stand alone and embedded in a website!
    paragraph = dwg.add(dwg.g(class_="fontyfont", ))
    paragraph.add(dwg.text("MICHAEL", insert=(0, 100)))
    dwg.save(pretty=True)

#create_font_template("designer.svg", "Inter", "Inter-V.ttf", 100, 750, 750)
create_webfont_font_template("designer.svg", 750, 750)
