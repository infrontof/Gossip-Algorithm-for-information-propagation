var clientId = '460806900716-u3eokqia6lqkaf7gpbtnjkln75v76deg.apps.googleusercontent.com';
var apiKey = 'AIzaSyCaIV8kmKkRs8pDk6P8jWqSGP9Saa9qDYA';
var scopes = 'https://www.googleapis.com/auth/calendar ';

//===========================================================
//OAtuh 2.0

function handleClientLoad() {
    gapi.client.setApiKey(apiKey);
    window.setTimeout(checkAuth, 1);
}

function checkAuth() {
    gapi.auth.authorize({ client_id: clientId, scope: scopes, immediate: true }, handleAuthResult);
}

function handleAuthResult(authResult) {
    var authorizeButton = document.getElementById('authorize-button');
    if (authResult && !authResult.error) {
        authorizeButton.style.visibility = 'hidden';
        makeApiCall();
    } else {
        authorizeButton.style.visibility = '';
        authorizeButton.onclick = handleAuthClick;
    }
}

function handleAuthClick(event) {
    gapi.auth.authorize({ client_id: clientId, scope: scopes, immediate: false }, handleAuthResult);
    return false;
}

//===========================================================
//api call

function makeApiCall() {
    gapi.client.load('calendar', 'v3', function () {
        var request = gapi.client.calendar.events.list({
            'calendarId': 'pingcise@gmail.com'
        });

        request.execute(function (resp) {
            $("#googleCalendarName").text(resp.summary);

            for (var i = 0; i < resp.items.length; i++) {
                var li = document.createElement('li');
                li.appendChild(document.createTextNode(resp.items[i].start.date + " " + resp.items[i].summary));
                document.getElementById('GoogleEvents').appendChild(li);
            }
        });
    });
}
