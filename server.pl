#!/usr/bin/env perl
use Mojolicious::Lite -signatures;
use DBI qw(:sql_types);

use constant DB => DBI->connect("dbi:Pg:service=IT1", '', '', {AutoCommit => 0, PrintError => 1, RaiseError => 1});
use constant PAGE_LIMIT => 100;

# > morbo server.pl

sub get_data($email) {
  my $data;
  eval {
    my $st = DB->prepare_cached(q{
        select int_id, created, str
        from message
        where int_id in (select distinct int_id from log where address = ?)
      union
        select int_id, created, str
        from log
        where int_id in (select distinct int_id from log where address = ?)
        order by int_id, created
      limit ?
    });
    $st->bind_param(1, $email,       SQL_VARCHAR);
    $st->bind_param(2, $email,       SQL_VARCHAR);
    $st->bind_param(3, PAGE_LIMIT+1, SQL_BIGINT);
    $st->execute;
    $data = $st->fetchall_arrayref({created => 1, str => 1, int_id => 1});
  };
  if ($@) {
    return ($@, undef);
  } else {
    return (undef, $data);
  }
}

get '/' => sub ($c) {
  $c->render(template => 'index');
};

get '/request' => sub ($c) {
  my $email = $c->param('email');
  my($err, $log) = get_data($email);
  if ($err) {
    $c->render(template => 'error', msg => "Ошибки при получении данных: $err");
  } else {
    my $truncated = 0;
    if (PAGE_LIMIT < scalar @$log) {
      $truncated = 1;
      pop @$log;
    }
    $c->render(template => 'response', email => $email, log => $log, truncated => $truncated);
  }
};

app->start;

__DATA__

@@ index.html.ep
% layout 'default';
% title 'Request';
<form action='/request' method='get'>
    Email: <input type='text' name='email'/> <input type='submit' value='Искать'/>
</form>

@@ layouts/default.html.ep
<!DOCTYPE html>
<html>
  <head><title><%= title %></title></head>
  <body><%= content %></body>
</html>

@@ response.html.ep
% layout 'response';
% title 'Response';
<h1>Активность, связанная с адресом <%= $email %></h1>
<h3>
Получено строк для отображения <%= scalar @$log %>.
<% if ($truncated) { %>
Часть строк отсечено!
<% } %>
</h3>
<table width="100%" border="1" cellpadding="4" cellspacing="0">
  <thead>
    <tr><th align='left'>Дата</th><th align='left'>Событие</th></tr>
  </thead>
  <tbody>
    <%
      # interlace rows background color for each int_id chain
      my @color = ('#ffffff', '#f0f0f0');
      my $color_index = 1;
      my $prev_id = '';

      for my $s (@$log) {
        if ($prev_id ne $s->{int_id}) {
          $color_index = ($color_index + 1) % 2;
        }
        $prev_id = $s->{int_id};
    %>
      <tr bgcolor='<%= $color[$color_index] %>'>
        <td><%= $s->{created}; %></td><td><%= $s->{str}; %></td>
      </tr>
    <% } %>
  </tbody>
</table>

@@ layouts/response.html.ep
<!DOCTYPE html>
<html>
  <head><title><%= title %></title></head>
  <body><%= content %></body>
</html>

@@ error.html.ep
% layout 'error';
% title 'Error';
<h1>Ошибка</h1>
<h3><%= $msg %></h3>

@@ layouts/error.html.ep
<!DOCTYPE html>
<html>
  <head><title><%= title %></title></head>
  <body><%= content %></body>
</html>
