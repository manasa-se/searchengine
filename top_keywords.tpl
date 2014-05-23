%if len(topKeywords) > 0:
	<p style='font-style:italic'>Top Keywords -</p>
	<table>
		<thead>
			<th>Word</th>
			<th>Search Count</th>
		</thead>
		<tbody>
	    % for row in topKeywords:
			<tr><td>{{row[0]}}</td><td>{{row[1]}}</td></tr>
		%end
		</tbody>
	</table>
%end