import React, { useRef, useState } from "react";
import {
  ActivityIndicator,
  Linking,
  Pressable,
  SafeAreaView,
  StatusBar,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { WebView } from "react-native-webview";
import type { WebViewNavigation } from "react-native-webview/lib/WebViewTypes";

const SITE_URL = process.env.EXPO_PUBLIC_DEFAULT_SERVER_URL || "https://letovoai.ru/";
const SITE_HOST = "letovoai.ru";

function isAllowedUrl(rawUrl: string): boolean {
  try {
    const url = new URL(rawUrl);
    return url.protocol === "https:" && url.hostname === SITE_HOST;
  } catch {
    return false;
  }
}

export default function App(): React.JSX.Element {
  const webViewRef = useRef<WebView>(null);
  const [loadingPage, setLoadingPage] = useState(true);
  const [loadError, setLoadError] = useState("");

  return (
    <SafeAreaView style={styles.root}>
      <StatusBar barStyle="light-content" backgroundColor="#232323" />
      <View style={styles.container}>
        <WebView
          ref={webViewRef}
          source={{ uri: SITE_URL }}
          style={styles.webView}
          originWhitelist={["https://*"]}
          sharedCookiesEnabled
          thirdPartyCookiesEnabled
          javaScriptEnabled
          domStorageEnabled
          pullToRefreshEnabled
          startInLoadingState
          setSupportMultipleWindows={false}
          mixedContentMode="never"
          onLoadStart={() => {
            setLoadingPage(true);
            setLoadError("");
          }}
          onLoadEnd={() => setLoadingPage(false)}
          onError={(event) => {
            setLoadingPage(false);
            setLoadError(event.nativeEvent.description || "Не удалось открыть сайт.");
          }}
          onNavigationStateChange={(nav: WebViewNavigation) => {
            if (nav.loading) {
              setLoadError("");
            }
          }}
          onShouldStartLoadWithRequest={(request) => {
            if (isAllowedUrl(request.url)) {
              return true;
            }
            void Linking.openURL(request.url);
            return false;
          }}
          renderLoading={() => (
            <View style={styles.overlay}>
              <ActivityIndicator size="large" color="#ff4b2b" />
              <Text style={styles.overlayTitle}>Letovo Assistant</Text>
              <Text style={styles.overlayText}>Открываю сайт…</Text>
            </View>
          )}
        />

        {!!loadError && (
          <View style={styles.errorOverlay}>
            <Text style={styles.errorTitle}>Сайт временно недоступен</Text>
            <Text style={styles.errorText}>{loadError}</Text>
            <Text style={styles.errorHint}>{SITE_URL}</Text>
            <Pressable style={styles.retryButton} onPress={() => webViewRef.current?.reload()}>
              <Text style={styles.retryButtonText}>Повторить</Text>
            </Pressable>
          </View>
        )}

        {loadingPage && !loadError ? (
          <View style={styles.loadingBadge}>
            <ActivityIndicator size="small" color="#ff4b2b" />
            <Text style={styles.loadingBadgeText}>Загрузка</Text>
          </View>
        ) : null}
      </View>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  root: {
    flex: 1,
    backgroundColor: "#232323",
  },
  container: {
    flex: 1,
    backgroundColor: "#232323",
  },
  webView: {
    flex: 1,
    backgroundColor: "#232323",
  },
  overlay: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    gap: 12,
    backgroundColor: "#232323",
    paddingHorizontal: 24,
  },
  overlayTitle: {
    color: "#f4f4f4",
    fontSize: 26,
    fontWeight: "800",
  },
  overlayText: {
    color: "#d0d0d0",
    fontSize: 15,
  },
  errorOverlay: {
    position: "absolute",
    inset: 0,
    alignItems: "center",
    justifyContent: "center",
    gap: 12,
    backgroundColor: "rgba(35, 35, 35, 0.97)",
    paddingHorizontal: 28,
  },
  errorTitle: {
    color: "#f4f4f4",
    fontSize: 22,
    fontWeight: "800",
    textAlign: "center",
  },
  errorText: {
    color: "#d6d6d6",
    fontSize: 15,
    lineHeight: 22,
    textAlign: "center",
  },
  errorHint: {
    color: "#ff7a60",
    fontSize: 14,
    textAlign: "center",
  },
  retryButton: {
    minHeight: 48,
    paddingHorizontal: 20,
    borderRadius: 14,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "#ff4b2b",
  },
  retryButtonText: {
    color: "#fff7f2",
    fontSize: 15,
    fontWeight: "700",
  },
  loadingBadge: {
    position: "absolute",
    top: 16,
    right: 16,
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 8,
    backgroundColor: "rgba(35, 35, 35, 0.9)",
  },
  loadingBadgeText: {
    color: "#f4f4f4",
    fontSize: 13,
    fontWeight: "700",
  },
});
