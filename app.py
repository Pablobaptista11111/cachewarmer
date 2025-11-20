// -------------------------------------------------------
// INTEGRAÇÃO V11: CACHE WARMER SNIPER
// -------------------------------------------------------

if ( ! defined( 'URL_ROBO_BASE' ) ) {
    define( 'URL_ROBO_BASE', 'https://python-cachewarmer.zcruu4.easypanel.host' ); 
}
if ( ! defined( 'TOKEN_ROBO' ) ) {
    define( 'TOKEN_ROBO', 'fullbai123' );
}

// CENÁRIO 1: SNIPER (Salva um produto específico)
add_action( 'save_post_product', 'fullbai_gatilho_unitario', 10, 3 );
function fullbai_gatilho_unitario( $post_id, $post, $update ) {
    // Evita revisões e autosaves
    if ( defined( 'DOING_AUTOSAVE' ) && DOING_AUTOSAVE ) return;
    if ( wp_is_post_revision( $post_id ) ) return;
    // Garante que o produto está publicado
    if ( get_post_status( $post_id ) != 'publish' ) return;

    // Pega a URL exata desse produto
    $url_produto = get_permalink( $post_id );

    // Manda o Sniper atacar só essa URL
    wp_remote_post( URL_ROBO_BASE . '/webhook_unitario', array(
        'blocking'  => false,
        'sslverify' => false,
        'headers'   => array('Content-Type' => 'application/json'),
        'body'      => json_encode( array(
            'token' => TOKEN_ROBO,
            'url'   => $url_produto
        ))
    ));
}

// CENÁRIO 2: GERAL (LiteSpeed Limpa Tudo)
add_action( 'litespeed_purged_all', 'fullbai_gatilho_geral' );
function fullbai_gatilho_geral() {
    // Dispara a varredura completa
    wp_remote_post( URL_ROBO_BASE . '/webhook?token=' . TOKEN_ROBO, array(
        'blocking'  => false,
        'sslverify' => false,
        'timeout'   => 0.01,
    ));
}
