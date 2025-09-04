from flask import url_for

def test_login_and_model_page(app_client, seeded):
    app, appmod = app_client
    client = app.test_client()

    with app.test_request_context():
        login_url = "/login"
        detail_url = app.url_for("model_detail", author=seeded["author"], model=seeded["model"])

    # not logged in: model page should still render (listing), downloads gated by session
    r = client.get(detail_url)
    assert r.status_code == 200

    # login
    r = client.post("/login", data={"email": seeded["user_email"], "password": seeded["user_pw"], "next": detail_url}, follow_redirects=True)
    assert r.status_code == 200
    assert seeded["user_email"] in r.get_data(as_text=True)

def test_permission_denied_when_no_perm(app_client, seeded):
    app, appmod = app_client
    client = app.test_client()
    db = appmod.get_db()

    # Create a user without permission
    db.execute("INSERT INTO users(email, name, role, pw_hash, active) VALUES(?,?,?,?,1)",
               ("noperm@example.com", "nop", "developer", appmod._make_password_hash("x")))
    db.commit()

    # login
    client.post("/login", data={"email": "noperm@example.com", "password": "x", "next": "/"}, follow_redirects=True)

    # attempt download
    rf = seeded["rows"][0]["rfilename"]
    r = client.get(f"/download/{seeded['repo_id']}/{rf}")
    assert r.status_code in (401, 403)  # gated
