package Net::LDAP::WithRetries;
our $VERSION = 0.08;
use 5.20.1;
use warnings;
use constant { 
    MAX_RETRIES_COUNT   => 3,
    MAX_RECON_TRIES     => 10,
    RECON_INTERVAL      => 0.01,
};
use IO::Socket;
use Errno;
use Net::LDAP;
use Scalar::Util qw(blessed refaddr);
use Time::HiRes qw(sleep);
use Data::Dumper;

sub new {
    my $class = shift;
    # save to avoid any further changes in the parent constructor
    my @args = @_;
    my $ldap_con = __connect(@_);
    
    # $self->SUPER is very slow, so we have to save parent's $ldapc inside the child instance    
    bless {args => \@args, ldapc => $ldap_con}, ref($class) || $class
}

our $AUTOLOAD;
sub AUTOLOAD {
    my $self = $_[0];
    my ($method) = $AUTOLOAD =~ /::([^:]+)$/;
    $self->{'bind_args'} = [@_[1..$#_]] if $method eq 'bind';
    
    no strict 'refs';
    
    *{$AUTOLOAD} =
    sub {
        my $self = shift;

        my $ldap_res; my $op_count = 0;
        while ($op_count++ < MAX_RETRIES_COUNT) {
            $ldap_res = $self->{'ldapc'}->$method(@_);
            ( blessed($ldap_res) and $ldap_res->isa('Net::LDAP::Message') )
                or do {
                    printf STDERR "Strange thing: method %s returns this instead of Net::LDAP::Message instance: %s\n", $method, Dumper([$ldap_res]);
                    last
                };
            if ( $ldap_res->code ) {
                unless ($ldap_res->error =~ /(?:Broken pipe|Connection)/i ) {
                    printf STDERR qq{Got LDAP error <<[%d] %s>>, but this is not "Broken pipe" or "Connection ...", so we are happy to do NOTHING :)\n}, $ldap_res->code, $ldap_res
                    last
                }
                say STDERR 'Ooops. Broken pipe!';
            } else { # all OK: this is very rare/unobvious case
                last
            }
            printf STDERR
                qq<LDAP connection is lost, will try to reconnect maximum %d times, with %s s. interval btw retries\n>,
                                                                MAX_RECON_TRIES,       RECON_INTERVAL;
            $self->{'ldapc'} = __connect( @{$self->{'args'}} );
            if( $self->{'bind_args'} and $method ne 'bind' ) {
                my $bind_res = $self->{'ldapc'}->bind( @{$self->{'bind_args'}} );
                $bind_res->code 
                    and die sprintf "failed to rebind: [%d] %s\n", $bind_res->code, $bind_res->error;
            }
        }
        return $ldap_res
    };
    goto &{$AUTOLOAD};
}

sub __connect {
    my $ldapc;
    my $recon_count = 0;
    do {
        sleep RECON_INTERVAL if $recon_count and ! $!{'ETIMEDOUT'};
        $ldapc = Net::LDAP->new(@_);
        $recon_count and 
            printf STDERR "LDAP reconnection try #%d: %s\n", 
                                                $recon_count,
                                                    $ldapc
                                                    ?           'success'
                                                    : sprintf   'failed by the reason: <<%s>>', $@;
    } until $ldapc or $recon_count++ >= MAX_RECON_TRIES;

    $ldapc or die sprintf 'LDAP reconnection failed after %d retries', $recon_count;
    $ldapc->socket->sockopt(SO_KEEPALIVE, 1);
    $ldapc
}

1;
