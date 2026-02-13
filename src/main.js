import { Actor } from 'apify';
import { PlaywrightCrawler } from 'crawlee';
import { parse } from 'csv-parse/sync';
import { readFileSync } from 'fs';

await Actor.init();

try {
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // INPUT取得
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    const input = await Actor.getInput();
    const {
        cookieStoreId = 'nvTNFxnnM87yDL9jC',
        storeId = '9d065554-e3c2-5f05-9869-3e2666b78fa2',
        storeName = 'BLA_NC HIROSHIMA',
        headless = true,
        debugMode = false,
    } = input || {};

    console.log('🚀 UberEats Manager CSVデータ収集開始');
    console.log(\`📥 Input:\`, JSON.stringify(input, null, 2));

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STEP 1: Cookie読み込み & 有効期限チェック
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    console.log('🍪 Cookieを読み込み中...');
    const cookieStore = await Actor.openKeyValueStore(cookieStoreId);
    const cookies = await cookieStore.getValue('cookies');

    if (!cookies || cookies.length === 0) {
        throw new Error('❌ Cookieが見つかりません');
    }
    console.log(\`✅ \${cookies.length}個のCookieを読み込みました\`);

    // sid Cookie（認証Cookie）の有効期限チェック
    const sidCookie = cookies.find(c => c.name === 'sid');
    if (!sidCookie) {
        throw new Error('❌ 認証Cookie（sid）が見つかりません');
    }

    const now = Date.now() / 1000;
    if (sidCookie.expirationDate && sidCookie.expirationDate > 0) {
        const daysRemaining = Math.floor((sidCookie.expirationDate - now) / 86400);
        console.log(\`✅ 認証Cookie（sid）有効期限: あと\${daysRemaining}日\`);
        
        if (daysRemaining <= 0) {
            throw new Error('🔴 Cookie期限切れ！至急更新してください。');
        }
        if (daysRemaining <= 7) {
            console.log(\`⚠️ Cookie残り\${daysRemaining}日。更新を準備してください。\`);
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STEP 2: Dataset準備（既存データ取得）
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    console.log('📊 Datasetを準備中...');
    
    const trendsDataset = await Actor.openDataset('ubereats-customer-trends');
    const locationsDataset = await Actor.openDataset('ubereats-customer-locations');
    
    // 既存データの差分チェック用Setを構築
    const existingTrends = await trendsDataset.getData();
    const existingTrendKeys = new Set(
        existingTrends.items.map(item => \`\${item.store_id}_\${item.date}\`)
    );
    console.log(\`  - 傾向データ: \${existingTrendKeys.size}件の既存レコード\`);
    
    const existingLocations = await locationsDataset.getData();
    const existingLocationKeys = new Set(
        existingLocations.items.map(item => 
            \`\${item.store_id}_\${item.period_start}_\${item.period_end}_\${item.pincode}\`
        )
    );
    console.log(\`  - 位置情報データ: \${existingLocationKeys.size}件の既存レコード\`);

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STEP 3: Playwright Crawlerでブラウザ起動
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    const targetUrl = \`https://merchants.ubereats.com/manager/home/\${storeId}/analytics/customers/?dateRangePreset=last_12_weeks\`;
    console.log(\`📄 対象URL: \${targetUrl}\`);

    let newTrendsCount = 0;
    let newLocationsCount = 0;

    const crawler = new PlaywrightCrawler({
        headless,
        launchContext: {
            launchOptions: {
                acceptDownloads: true, // ダウンロード有効化
            },
        },
        preNavigationHooks: [
            async ({ page }, goToOptions) => {
                console.log('🍪 Cookieをブラウザに注入中...', goToOptions.url);
                await page.context().addCookies(cookies);
                console.log(\`✅ \${cookies.length}個のCookieを注入しました\`);
            },
        ],
        requestHandler: async ({ page, request }) => {
            console.log(\`🌐 ページにアクセス: \${request.url}\`);

            // ページ読み込み待機
            await page.waitForTimeout(2000);

            const currentUrl = page.url();
            console.log(\`🔍 現在のURL: \${currentUrl}\`);

            const title = await page.title();
            console.log(\`📄 ページタイトル: \${title}\`);

            // ログインチェック
            if (currentUrl.includes('login')) {
                throw new Error('❌ ログイン失敗：Cookieが無効です');
            }

            console.log('✅ ログイン成功！注文者分析データページが表示されています');

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // STEP 4: 位置情報CSV ダウンロード
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            console.log('');
            console.log('📍 位置情報CSVをダウンロード中...');
            
            // 「注文者の位置情報」セクションまでスクロール
            await page.evaluate(() => {
                const element = document.evaluate(
                    "//*[contains(text(), '注文者の位置情報')]",
                    document,
                    null,
                    XPathResult.FIRST_ORDERED_NODE_TYPE,
                    null
                ).singleNodeValue;
                if (element) element.scrollIntoView({ behavior: 'smooth', block: 'center' });
            });
            await page.waitForTimeout(2000);

            // ダウンロードボタンを探してクリック
            const locationDownloadPromise = page.waitForEvent('download');
            
            // XPath または text で「注文者の位置情報」の近くにある「ダウンロード」ボタンを探す
            await page.locator('text=注文者の位置情報').locator('..').locator('button:has-text("ダウンロード")').click();
            
            const locationDownload = await locationDownloadPromise;
            console.log(\`✅ 位置情報CSVダウンロード開始: \${locationDownload.suggestedFilename()}\`);

            // ファイルを読み込み
            const locationPath = await locationDownload.path();
            const locationCsvContent = readFileSync(locationPath, 'utf-8');
            console.log(\`📄 位置情報CSV読み込み完了 (\${locationCsvContent.length} bytes)\`);

            // CSVパース（日本語ヘッダー）
            const locationRecords = parse(locationCsvContent, {
                columns: true,
                skip_empty_lines: true,
                bom: true, // BOM対応
            });
            console.log(\`📊 位置情報レコード数: \${locationRecords.length}\`);

            // 期間情報を取得（URLから）
            const urlParams = new URL(request.url).searchParams;
            const periodPreset = urlParams.get('dateRangePreset') || 'last_12_weeks';
            const periodStart = urlParams.get('start') || '';
            const periodEnd = urlParams.get('end') || '';

            // Dataset保存用に変換（差分チェック付き）
            const newLocationRecords = [];
            for (const row of locationRecords) {
                const pincode = row['郵便番号'] || row['pincode'] || '';
                const key = \`\${storeId}_\${periodStart}_\${periodEnd}_\${pincode}\`;
                
                if (!existingLocationKeys.has(key)) {
                    newLocationRecords.push({
                        store_id: storeId,
                        store_name: storeName,
                        period_preset: periodPreset,
                        period_start: periodStart,
                        period_end: periodEnd,
                        pincode: pincode,
                        new_customers: parseInt(row['新着'] || row['new'] || 0),
                        occasional_customers: parseInt(row['低頻度'] || row['occasional'] || 0),
                        frequent_customers: parseInt(row['高頻度'] || row['frequent'] || 0),
                        total: parseInt(row['すべて'] || row['total'] || 0),
                        downloaded_at: new Date().toISOString(),
                    });
                }
            }

            if (newLocationRecords.length > 0) {
                await locationsDataset.pushData(newLocationRecords);
                console.log(\`✅ 位置情報: \${newLocationRecords.length}件の新規レコードを保存\`);
                newLocationsCount = newLocationRecords.length;
            } else {
                console.log('ℹ️  位置情報: 新規レコードなし（すべて既存データ）');
            }

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // STEP 5: 傾向CSV ダウンロード
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            console.log('');
            console.log('📈 注文者グループの傾向CSVをダウンロード中...');
            
            // 「注文者グループの傾向」セクションまでスクロール
            await page.evaluate(() => {
                const element = document.evaluate(
                    "//*[contains(text(), '注文者グループの傾向')]",
                    document,
                    null,
                    XPathResult.FIRST_ORDERED_NODE_TYPE,
                    null
                ).singleNodeValue;
                if (element) element.scrollIntoView({ behavior: 'smooth', block: 'center' });
            });
            await page.waitForTimeout(2000);

            // ダウンロードボタンを探してクリック
            const trendsDownloadPromise = page.waitForEvent('download');
            
            await page.locator('text=注文者グループの傾向').locator('..').locator('button:has-text("ダウンロード")').click();
            
            const trendsDownload = await trendsDownloadPromise;
            console.log(\`✅ 傾向CSVダウンロード開始: \${trendsDownload.suggestedFilename()}\`);

            // ファイルを読み込み
            const trendsPath = await trendsDownload.path();
            const trendsCsvContent = readFileSync(trendsPath, 'utf-8');
            console.log(\`📄 傾向CSV読み込み完了 (\${trendsCsvContent.length} bytes)\`);

            // CSVパース
            const trendsRecords = parse(trendsCsvContent, {
                columns: true,
                skip_empty_lines: true,
                bom: true,
            });
            console.log(\`📊 傾向レコード数: \${trendsRecords.length}\`);

            // Dataset保存用に変換（差分チェック付き）
            const newTrendsRecords = [];
            for (const row of trendsRecords) {
                const date = row['Date'] || row['date'] || '';
                const key = \`\${storeId}_\${date}\`;
                
                if (!existingTrendKeys.has(key)) {
                    const newVal = parseInt(row['New'] || row['new'] || 0);
                    const freqVal = parseInt(row['Frequent'] || row['frequent'] || 0);
                    const occVal = parseInt(row['Occasional'] || row['occasional'] || 0);
                    
                    newTrendsRecords.push({
                        store_id: storeId,
                        store_name: storeName,
                        date: date,
                        new_customers: newVal,
                        frequent_customers: freqVal,
                        occasional_customers: occVal,
                        total: newVal + freqVal + occVal,
                        downloaded_at: new Date().toISOString(),
                    });
                }
            }

            if (newTrendsRecords.length > 0) {
                await trendsDataset.pushData(newTrendsRecords);
                console.log(\`✅ 傾向データ: \${newTrendsRecords.length}件の新規レコードを保存\`);
                newTrendsCount = newTrendsRecords.length;
            } else {
                console.log('ℹ️  傾向データ: 新規レコードなし（すべて既存データ）');
            }

            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            // STEP 6: デバッグ情報保存（debugMode時のみ）
            // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            if (debugMode) {
                console.log('📸 スクリーンショットを保存中...');
                await Actor.setValue('screenshot_success', await page.screenshot({ fullPage: true }), {
                    contentType: 'image/png',
                });
                
                await Actor.setValue('page_html', await page.content(), {
                    contentType: 'text/html',
                });
                console.log('✅ デバッグ情報を保存しました');
            }

            console.log('');
            console.log('✅ CSVダウンロード完了！');
            console.log('');
            console.log('📊 結果サマリー:');
            console.log(\`   位置情報: \${newLocationsCount}件の新規レコード\`);
            console.log(\`   傾向データ: \${newTrendsCount}件の新規レコード\`);
        },
        maxRequestsPerCrawl: 1,
    });

    await crawler.run([targetUrl]);

    console.log('');
    console.log('🎉 すべて完了しました！');
    
} catch (error) {
    console.error('');
    console.error('❌ エラーが発生しました:');
    console.error(error.message);
    console.error('');
    throw error;
}

await Actor.exit();
