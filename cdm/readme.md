# ```/cdm```

Main applications for this project. "Central Decision Maker" as said gemini.

* Don't put a lot of code in the application directory:
    - If you think the code can be imported and used in other projects, then it should live in the /pkg directory. 
    - If the code is not reusable or if you don't want others to reuse it, put that code in the /internal directory. 
    - You'll be surprised what others will do, so be explicit about your intentions!