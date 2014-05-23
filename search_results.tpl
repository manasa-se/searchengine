%if len(searchResults) > 0:
	%for result in searchResults:
		<div id='result'>
		<p>{{result[1]}}</p>
		% if result[0][-1] != '/':
			<a href={{str(result[0])}}>{{str(result[0])}}</a><br/>
		%else:
			<a href={{str(result[0][:-1])}}>{{str(result[0][:-1])}}</a><br/>
		%end
		<p>{{result[2]}}<br/>
		<hr/>
		</div>
	%end
%else:
	<p>No results found</p>
%end