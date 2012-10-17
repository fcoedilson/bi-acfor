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
####backup_table("fato_indicador");

# lendo diretórios
while (my $file = readdir(DIR)) {
	##print "Lendo $file ........................................ \n";
	next if ($file =~ m/^\./);  # Use a regular expression to ignore files beginning with a period
	ler_arquivo($dir, $file);
	##print "Arquivo $file foi salvo.............................\n";
}


#INSERT INTO fato_indicador(
#	id_dim_tempo, 
#	id_dim_municipio, 
#	id_dim_unidade_administrativa, 
#	id_dim_indicador, 
#	valor_indicador) 
#(select t.id, 
#	(select m.id from dim_municipio m where m.codigo = 9869), 
#	(select u.id from dim_unidade_administrativa u where u.codigo = 48) , 
#	(select v.id from dim_variavel v where v.codigo = 3641) , 3550 
#from dim_tempo t where t.ano_mes = 201112)


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
			my $insert = "INSERT INTO fato_indicador(
				id_dim_tempo, 
				id_dim_municipio, 
				id_dim_unidade_administrativa, 
				id_dim_indicador, 
				valor_indicador) 
			select id, $local, $unid, $cdvar, $vlvar from dim_tempo where ano_mes = $ano$mes;";
			print "$insert \n";
			
			#### salvar_registro("$insert") or die "Erro ao tentar inserir registro ...";
		}
		$cont++;
	}
	close(FILE);
}

#INSERT INTO fato_indicador(id_dim_tempo, id_dim_municipio, id_dim_unidade_administrativa, 
#id_dim_indicador, valor_indicador) select id, 1, 84, 11, 2323.8 from dim_tempo where ano_mes = 201010;


# salvando registro
sub salvar_registro{
	my ($row) = @_;
	## print "Salvando registro [$row ]...\n";
	my $query = $dbi->prepare($row) || die $dbi->errstr;
	$query->execute() or die "Não foi possível inserir registro ...";
	$query->finish();
	## print "Salvou ....\n";
}

closedir(DIR);


# id_dim_tempo bigint,
# id_dim_municipio bigint,
# id_dim_unidade_administrativa bigint,
# id_dim_indicador bigint,
# valor_indicador numeric(16,2),

# limpa tabelas
sub backup_table{

    my ($tb) = @_;
    my ($sec,$min,$hour,$day,$mon,$year,$wday,$yday,$isdst) = localtime time;
    $mon = $mon+1;

    my $dat = "$year$mon$day";
    my $hor = "$hour$min";
    my $suf = "$dat" . "_" . "$hor";

    my $sth_queues = $dbi->prepare("ALTER TABLE fato_indicador RENAME TO fato_indicador_$suf;");
    $sth_queues->execute() or die "Não foi possível renomear tabela ...";
    print "Tabela  [fato_indicador] renomeada!\n";
	
	## TODO: VERIFICAR SE A TABALA EXISTE. CASO A MESMA NÃO EXISTE VAI GERAR ERRO
	 
   $sth_queues = $dbi->prepare("CREATE TABLE fato_indicador(
		id bigserial NOT NULL,
		id_dim_tempo bigint,
		id_dim_municipio bigint, 
		id_dim_unidade_administrativa bigint, 
		id_dim_indicador bigint , 
		valor_indicador numeric(16,2),
	   CONSTRAINT pk_fato_indicador_$suf PRIMARY KEY (id))") or die $dbi->errstr;
    
   $sth_queues->execute() or die "Não foi possível criar tabela ...";
   print "Tabela  [fato_indicador] criada!\n";
}


exit 0;
