<html>
<head>
  <link rel="stylesheet" type="text/css" href="/static/style.css" />
  <title>Quaero Search</title>


<script>
function checkLoggedIn()
{
  var login=getCookie("login");
  if (login!=null && login!="")
  {
    document.getElementById('signinButton').setAttribute('style', 'display: none');
    document.getElementById('searchBar').setAttribute('style', 'display: block');
    document.getElementById('logout').setAttribute('style', 'display: block');
  }
  else 
  {
    document.getElementById('signinButton').setAttribute('style', 'display: block');
    document.getElementById('searchBar').setAttribute('style', 'display: none');
    document.getElementById('logout').setAttribute('style', 'display: none');
  }
}


</script>

</head>
<body onload="checkLoggedIn()">
  <div id='wrapper'>
   <div id='searchHeader'>
    <div><h1 id='searchTitle'>quaero</h1></div>
    <span id="signinButton">
      <span 
      class="g-signin"
      data-callback="signinCallback"
      data-clientid="147046765542-a416holoun4dr48tqu2ged8b4e41pm0l.apps.googleusercontent.com"
      data-cookiepolicy="single_host_origin"
      data-requestvisibleactions="http://schemas.google.com/AddActivity"
      data-scope="https://www.googleapis.com/auth/plus.login"></span>
    </span>
    <div id='searchBar'>
     <form class="form-wrapper searchBar">
      <input type = "text" name="searchInput" placeholder="Search here..." required />
      <button type = "submit"/>
    </form>
  </div>
</div>
<input id='logout' type='button' onclick="gapi.auth.signOut();document.getElementById('signinButton').setAttribute('style', 'display: block');document.getElementById('searchBar').setAttribute('style', 'display: none');setCookie('login','',1);checkLoggedIn();" value='Gmail Log Out'/>
<div id='topKeywords'>
  <br/><br/>
</div>
</div>

<div id='result-wrapper'>
 <div id='result-content' style="overflow-y:auto;">
 </div>
</div>
</body>
<script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.4.2/jquery.min.js"></script>
<script type="text/javascript">
function scrollalert(){  
   var scrolltop=$('#result-content').attr('scrollTop');  
   var scrollheight=$('#result-content').attr('scrollHeight');  
   var windowheight=$('#result-content').attr('clientHeight');  
   var scrolloffset=20;  
   if(scrolltop>=(scrollheight-(windowheight+scrolloffset)))  
   {  
     $.ajax({
      type: 'GET',
      url: '/ajaxSearchResults',  
      success: function(response) {
        $('#result-content').append(response);
      }
    });  
   }  
   setTimeout('scrollalert();', 1500);  
 }  

 function getCookie(c_name)
 {
  var c_value = document.cookie;
  var c_start = c_value.indexOf(" " + c_name + "=");
  if (c_start == -1)
  {
    c_start = c_value.indexOf(c_name + "=");
  }
  if (c_start == -1)
  {
    c_value = null;
  }
  else
  {
    c_start = c_value.indexOf("=", c_start) + 1;
    var c_end = c_value.indexOf(";", c_start);
    if (c_end == -1)
    {
      c_end = c_value.length;
    }
    c_value = unescape(c_value.substring(c_start,c_end));
  }
  return c_value;
}

function setCookie(c_name,value,exdays)
{
  var exdate=new Date();
  exdate.setDate(exdate.getDate() + exdays);
  var c_value=escape(value) + ((exdays==null) ? "" : "; expires="+exdate.toUTCString());
  document.cookie=c_name + "=" + c_value;
}

$(document).ready(function() {
 scrollalert();

 $('form').submit(function(e) {
  $.ajax({
    type: 'GET',
    url: '/ajaxSearchResults',
    data: { searchInput: $(this).serialize(), from: "form_submit" },
    success: function(response) {
      $('#result-content').html(response);
    }
  });
  $.ajax({
    type: 'GET',
    url: '/ajaxUpdateTopKeywords',
    data: $(this).serialize(),
    success: function(response) {
      $('#topKeywords').html(response);
    }
  });
  e.preventDefault();
});
});

(function() {
 var po = document.createElement('script'); po.type = 'text/javascript'; po.async = true;
 po.src = 'https://apis.google.com/js/client:plusone.js';
 var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(po, s);
})();

function signinCallback(authResult) {
  if (authResult['access_token']) {
    // Update the app to reflect a signed in user
    // Hide the sign-in button now that the user is authorized, for example:
    document.getElementById('signinButton').setAttribute('style', 'display: none');
    document.getElementById('searchBar').setAttribute('style', 'display: block');
    setCookie("login","login",1);
    checkLoggedIn();
  } else if (authResult['error']) {
    // Update the app to reflect a signed out user
    // Possible error values:
    //   "user_signed_out" - User is signed-out
    //   "access_denied" - User denied access to your app
    //   "immediate_failed" - Could not automatically log in the user
    console.log('Sign-in state: ' + authResult['error']);
  }
}
function disconnectUser(access_token) {
  var revokeUrl = 'https://accounts.google.com/o/oauth2/revoke?token=' +
  access_token;

  // Perform an asynchronous GET request.
  $.ajax({
    type: 'GET',
    url: revokeUrl,
    async: false,
    contentType: "application/json",
    dataType: 'jsonp',
    success: function(nullResponse) {
      // Do something now that user is disconnected
      // The response is always undefined.
    },
    error: function(e) {
      // Handle the error
      // console.log(e);
      // You could point users to manually disconnect if unsuccessful
      // https://plus.google.com/apps
    }
  });
}
// Could trigger the disconnect on a button click
$('#revokeButton').click(disconnectUser);
</script>
</html>