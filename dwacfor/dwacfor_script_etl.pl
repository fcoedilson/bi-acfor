#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Data::Dumper;

# Instalação do DBI::Pg 
# sudo cpan
# notest install DBD
# notest install DBD::Pg

my $dsn = 'DBI:Pg:dbname=dw-acfor;host=172.17.75.101';
my $userid = 'dwacfor';
my $password = 'dwacfor';
my $dbi = DBI->connect($dsn, $userid, $password, {AutoCommit => 1}) or die $DBI::errstr;

my $dir = '/opt/dwacfor/ftp_files/';
my @files_list;
opendir(DIR, $dir) or die $!;

# apagando os dados da tabela
backup_table("ods_valores_variaveis");

# lendo diretórios
while (my $file = readdir(DIR)) {
	print "Lendo $file ........................................ \n";
	next if ($file =~ m/^\./);  # Use a regular expression to ignore files beginning with a period
	ler_arquivo($dir, $file);
##	print "Arquivo $file foi salvo.............................\n";
}

# lendo arquivo
sub ler_arquivo {

	my ($dir, $name) = @_;
	my $fpath = $dir.$name;
	my $cont = 0;

	open(FILE, $fpath);
	while (<FILE>) {
		chomp;
		if($cont > 1){
			my($setor, $ano, $mes, $local, $unid, $cdvar, $vlvar) = split(";");
			$vlvar =~ s/,/./;
			my $insert = "INSERT INTO ods_valores_variaveis(setor, ano_referencia, mes_referencia, localidade, unidade, codigo_variavel, valor_variavel, numero_linha_arquivo) VALUES($setor, $ano, $mes, $local, $unid, $cdvar, $vlvar, $cont+1);";
			salvar_registro("$insert") or die "Erro ao tentar inserir registro ...";
		}
		$cont++;
	}
	close(FILE);
}

# salvando registro
sub salvar_registro{
	my ($row) = @_;
	print "Salvando registro [$row ]...\n";
	my $query = $dbi->prepare($row) || die $dbi->errstr;
	$query->execute() or die "Não foi possível inserir registro ...";
	$query->finish();
	print "Salvou ....\n";
}

closedir(DIR);

# limpa tabelas
sub backup_table{

    my ($tb) = @_;
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime time;
    
    my $sth_queues = $dbi->prepare("ALTER TABLE ods_valores_variaveis RENAME TO ods_valores_variaveis$mon$mday$hour$min$sec;");
    $sth_queues->execute() or die "Não foi possível renomear tabelas ...";
    print "Tabela  [ods_valores_variaveis] renomeada!\n";
	
	## TODO: VERIFICAR SE A TABALA EXISTE. CASO A MESMA NÃO EXISTE VAI GERAR ERRO
	 
   $sth_queues = $dbi->prepare("CREATE TABLE ods_valores_variaveis(setor character varying(2) NOT NULL, 
	ano_referencia integer NOT NULL, mes_referencia integer NOT NULL, localidade integer NOT NULL, unidade integer NOT NULL, 
	codigo_variavel integer NOT NULL, valor_variavel character varying(100), numero_linha_arquivo integer, 
	CONSTRAINT pk_ods_vars_$mon$mday$hour$min$sec PRIMARY KEY (setor , ano_referencia , mes_referencia , localidade , unidade, codigo_variavel ))") or die $dbi->errstr;
    
   $sth_queues->execute() or die "Não foi possível criar tabela ...";
   print "Tabela  [ods_valores_variaveis] criada!\n";
}


exit 0;
