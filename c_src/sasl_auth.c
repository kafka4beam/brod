#include <erl_nif.h>
#include <stdio.h>
#include <string.h>
#include <sasl/sasl.h>
#include <krb5.h>

// MIT is broken
#ifdef KRB5_KRB5_H_INCLUDED
#define KRB5KDC_ERR_KEY_EXPIRED KRB5KDC_ERR_KEY_EXP
#endif

// Structure for kerberos authentication
struct kebab
{
  krb5_error_code error;
  krb5_principal  principal;
  krb5_context    context;
  krb5_creds      creds;
};

// SASL connection
sasl_conn_t *conn;

// Gets existing atom or creates new
ERL_NIF_TERM get_atom(ErlNifEnv* env, const char* atom_name);

// Creates tuple of form {error, Reason} with specified reason
ERL_NIF_TERM make_error(ErlNifEnv* env, char *tag);

// Cleans kerberos context and returns error
ERL_NIF_TERM error_and_exit(ErlNifEnv* env, struct kebab *kebab, char *tag);

// Lib unload callback
void unload(ErlNifEnv* env, void* priv_data);

ERL_NIF_TERM get_atom(ErlNifEnv* env, const char* atom_name)
{
    ERL_NIF_TERM atom;
    if(enif_make_existing_atom(env, atom_name, &atom, ERL_NIF_LATIN1))
        return atom;
    return enif_make_atom(env, atom_name);
}

ERL_NIF_TERM make_error(ErlNifEnv* env, char *tag)
{
	ERL_NIF_TERM return_atom = get_atom(env, "error");
	ERL_NIF_TERM reason = enif_make_string(env, tag, ERL_NIF_LATIN1);

    return enif_make_tuple2(env, return_atom, reason);
}

ERL_NIF_TERM error_and_exit(ErlNifEnv* env, struct kebab *kebab, char *tag)
{
    char fmt_error_msg[1024];
    const char * krb_error_msg = krb5_get_error_message(kebab->context, kebab->error);
    memset(fmt_error_msg, 0, (int)sizeof(fmt_error_msg));
    snprintf(fmt_error_msg, (int)sizeof(fmt_error_msg), "%s: (%i) %s", tag, kebab->error, krb_error_msg);
    ERL_NIF_TERM error_message = enif_make_string(env, fmt_error_msg, ERL_NIF_LATIN1);
    ERL_NIF_TERM return_atom;

    if (krb_error_msg != NULL)
        krb5_free_error_message(kebab->context, krb_error_msg);

    if (kebab->error == 0)
		return_atom = get_atom(env, "ok");
    else if (kebab->error == KRB5KDC_ERR_C_PRINCIPAL_UNKNOWN ||
           kebab->error == KRB5KRB_AP_ERR_BAD_INTEGRITY ||
           kebab->error == KRB5KDC_ERR_PREAUTH_FAILED ||
           kebab->error == KRB5KDC_ERR_KEY_EXPIRED
          )
        return_atom = get_atom(env, "refused");
    else
        return_atom = get_atom(env, "error");

    if (kebab->context)
    {
        if (&kebab->creds)
            krb5_free_cred_contents(kebab->context, &kebab->creds);
        if (kebab->principal)
            krb5_free_principal(kebab->context, kebab->principal);

        krb5_free_context(kebab->context);
  }

    return enif_make_tuple2(env, return_atom, error_message);
}

static int sasl_cyrus_cb_getsimple(void *context, int id, const char **result, unsigned *len)
{
	switch (id)
	{
		case SASL_CB_USER:
			*result = context;
		case SASL_CB_AUTHNAME:
	        *result = context;
	    break;
		default:
	        *result = NULL;
	    break;
	}

	if (len)
		*len = *result ? strlen(*result) : 0;

	return *result ? SASL_OK : SASL_FAIL;
}

static ERL_NIF_TERM sasl_cli_init(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	int result;
	result = sasl_client_init(NULL);
	return enif_make_int(env, result);
}

static ERL_NIF_TERM sasl_cli_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary service;
    ErlNifBinary host;
    ErlNifBinary principal;

    if (conn)
		sasl_dispose(&conn);

    if (!enif_is_binary(env, argv[0]) || !enif_is_binary(env, argv[1]) || !enif_is_binary(env, argv[2]))
        return enif_make_badarg(env);

    if (!enif_inspect_binary(env, argv[0], &service) || !enif_inspect_binary(env, argv[1], &host) || !enif_inspect_binary(env, argv[2], &principal))
        return enif_make_badarg(env);

    service.data[service.size] = '\0';
    host.data[host.size] = '\0';
    principal.data[principal.size] = '\0';

    sasl_callback_t callbacks[16] =
    	{
    		{ SASL_CB_USER,         (void *)sasl_cyrus_cb_getsimple, principal.data },
            { SASL_CB_AUTHNAME,     (void *)sasl_cyrus_cb_getsimple, principal.data },
            { SASL_CB_LIST_END }
    	};

    int result;
	result = sasl_client_new((const char *)service.data, (const char *)host.data, NULL, NULL, callbacks, 0, &conn);
	return enif_make_int(env, result);
}

static ERL_NIF_TERM sasl_list_mech(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	const char *avail_mechs;
	sasl_listmech(conn, NULL, NULL, " ", NULL, &avail_mechs, NULL, NULL);
	return enif_make_string(env, avail_mechs, ERL_NIF_LATIN1);
}

static ERL_NIF_TERM sasl_cli_start(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	int result;
	const char *out, *mech;
	unsigned int outlen;
	result = sasl_client_start(conn, "GSSAPI", NULL, &out, &outlen, &mech);
	return enif_make_tuple2(env, enif_make_int(env, result), enif_make_string_len(env, out, outlen, ERL_NIF_LATIN1));
}

static ERL_NIF_TERM sasl_cli_step(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary challenge;

    if (!enif_is_binary(env, argv[0]))
        return enif_make_badarg(env);

    if (!enif_inspect_binary(env, argv[0], &challenge))
        return enif_make_badarg(env);

	int result;
	sasl_interact_t *interact = NULL;
    const char *out;
    unsigned int outlen;
    char* in = NULL;
    if (challenge.size > 0)
    {
		in = (char*)malloc(challenge.size);
		memcpy(in, challenge.data, challenge.size);
	}
	result = sasl_client_step(conn,
                             in, challenge.size,
                             &interact,
                             &out, &outlen);
	free(in);

	return enif_make_tuple2(env, enif_make_int(env, result), enif_make_string_len(env, out, outlen, ERL_NIF_LATIN1));
}

static ERL_NIF_TERM sasl_error(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
	return enif_make_string(env, sasl_errdetail(conn), ERL_NIF_LATIN1);
}

static ERL_NIF_TERM kinit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    struct kebab kebab;

    ErlNifBinary keytab;
    ErlNifBinary principal;

    if (!enif_is_binary(env, argv[0]) || !enif_is_binary(env, argv[1]))
        return enif_make_badarg(env);

    if (!enif_inspect_binary(env, argv[0], &keytab) || !enif_inspect_binary(env, argv[1], &principal))
        return enif_make_badarg(env);

    keytab.data[keytab.size] = '\0';
    principal.data[principal.size] = '\0';

    if ((kebab.error = krb5_init_context(&kebab.context)))
        return error_and_exit(env, &kebab, "krb5_init_context");

    memset(&kebab.creds, 0, sizeof(krb5_creds));

    if ((kebab.error = krb5_parse_name(kebab.context, (char*)principal.data, &kebab.principal)))
        return error_and_exit(env, &kebab, "krb5_parse_name");

    krb5_keytab ktHnd;
    if ((kebab.error = krb5_kt_resolve(kebab.context, (char *)keytab.data, &ktHnd)))
        return error_and_exit(env, &kebab, "krb5_kt_resolve");

	if ((kebab.error = krb5_get_init_creds_keytab(kebab.context,&kebab.creds,kebab.principal,ktHnd,0,NULL,NULL)))
	{
		krb5_kt_close(kebab.context, ktHnd);
		return error_and_exit(env, &kebab, "krb5_get_init_creds_keytab");
	}
	krb5_kt_close(kebab.context, ktHnd);

    return error_and_exit(env, &kebab, NULL);
}

void unload(ErlNifEnv* env, void* priv_data)
{
	if (conn)
		sasl_dispose(&conn);
}

static ErlNifFunc nif_funcs[] =
{
	{"sasl_client_init", 0, sasl_cli_init},
	{"sasl_client_new", 3, sasl_cli_new},
	{"sasl_listmech", 0, sasl_list_mech},
	{"sasl_client_start", 0, sasl_cli_start},
	{"sasl_client_step", 1, sasl_cli_step},
	{"sasl_errdetail", 0, sasl_error},
	{"kinit", 2, kinit}
};

ERL_NIF_INIT(sasl_auth, nif_funcs, NULL, NULL, NULL, &unload)
