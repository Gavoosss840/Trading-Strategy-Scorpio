# Modigliani-Miller Adjusted Trading Strategy

## 📖 Théorie

Le théorème de Modigliani-Miller (1958) établit qu'en marché parfait, la valeur d'une entreprise est indépendante de sa structure de capital. Cependant, dans la réalité, plusieurs facteurs créent des déviations :

### Formule Théorique Ajustée

```
VL = VU + PV(Tax Shield) - PV(Financial Distress) - PV(Agency Costs)
```

**Où :**
- **VL** = Valeur de l'entreprise endettée (Levered Value)
- **VU** = Valeur de l'entreprise sans dette (Unlevered Value)
- **Tax Shield** = Économies d'impôts liées à la déductibilité des intérêts
- **Financial Distress** = Coûts de détresse financière (faillite potentielle)
- **Agency Costs** = Coûts liés aux conflits actionnaires/créanciers

### Composantes du Modèle

#### 1. Tax Shield (Bouclier Fiscal)
```
Annual Tax Shield = Taux d'Imposition × Intérêts Payés
PV(Tax Shield) = Annual Tax Shield / (rf + prime de risque)
```

**Impact :** Positif - L'endettement crée de la valeur via les économies d'impôts

#### 2. Financial Distress Costs (Coûts de Détresse)

Utilise le **Modèle de Merton** pour calculer la probabilité de défaut :

```
P(défaut) = N(-d2)

où d2 = [ln(V/D) + (μ - 0.5σ²)T] / (σ√T)

V = Valeur des actifs (Enterprise Value)
D = Dette
σ = Volatilité des actifs
T = Horizon (1 an)
```

**Coûts estimés :**
- Coûts directs : 3-5% de la valeur de la firme (frais légaux, restructuration)
- Coûts indirects : 10-20% (perte clients, fournisseurs, employés)
- **Total utilisé : 20%** en cas de détresse

```
Expected Distress Cost = P(défaut) × 20% × Firm Value
```

**Impact :** Négatif - Un endettement excessif augmente le risque de faillite

#### 3. Agency Costs (Coûts d'Agence)

Conflits entre actionnaires et créanciers qui augmentent avec :
- **Leverage élevé** (D/E > 2)
- **Free Cash Flow élevé** (risque de surinvestissement)
- **Opportunités de croissance limitées**

```
Agency Score = f(Leverage, FCF/MarketCap)
PV(Agency Costs) = Agency Score × Market Cap
```

**Impact :** Négatif - Plus de dette = plus de conflits d'intérêts

## 🎯 Stratégie de Trading

### Signal de Trading

1. **Calcul de la divergence :**
   ```
   Divergence (%) = (VL théorique - Market Cap) / Market Cap × 100
   ```

2. **Génération du signal :**
   - **BUY** : Si Divergence > +10% → Entreprise sous-évaluée
   - **SELL** : Si Divergence < -10% → Entreprise surévaluée
   - **HOLD** : Si -10% ≤ Divergence ≤ +10% → Valorisation équilibrée

3. **Calcul de la confiance :**
   ```
   Confidence = min(|Divergence| × 10, 100)
   ```
   - Divergence de 15% → Confidence de 100%
   - Divergence de 5% → Confidence de 50%

### Position Sizing Dynamique

Le sizing est **proportionnel à la confiance** :

```
Position Size (%) = 1% + (5% - 1%) × [(Confidence - 20) / (100 - 20)]
```

**Exemples :**
- Confidence 20% → Position 1% du capital
- Confidence 60% → Position 3% du capital
- Confidence 100% → Position 5% du capital

**Minimum requis :** Confidence ≥ 20% pour trader

## 📁 Fichiers

### 1. `M-M theroem application.py`
**Modèle d'analyse et screening (sans trading automatique)**

**Fonctionnalités :**
- Analyse détaillée d'une entreprise
- Screening du S&P 500
- Export CSV des résultats

**Utilisation :**
```bash
python "M-M theroem application.py"
```

**Output :**
- Données financières (Market Cap, Dette, D/E ratio, etc.)
- Valorisation M-M (VU, Tax Shield, Distress Costs, Agency Costs)
- VL Théorique vs Market Cap
- Signal : BUY/SELL/HOLD
- Confiance (0-100%)

### 2. `MM_Trading_Bot.py`
**Bot de trading automatique (avec connexion IBKR)**

**Fonctionnalités :**
- Scanne le S&P 500 en continu (toutes les 5 min par défaut)
- **Récupère la liste S&P 500 directement depuis IBKR** (garantit que tous les tickers sont tradeables)
- Détecte les opportunités (divergence > 10%)
- Execute automatiquement les trades via Interactive Brokers
- Position sizing dynamique (1-5% selon confidence)

**Utilisation :**
```bash
python "MM_Trading_Bot.py"
```

**Prérequis :**
1. **TWS ouvert** et connecté
2. **API activée** : File → Global Configuration → API → Enable ActiveX and Socket Clients
3. **Port correct** :
   - 7497 = Paper Trading
   - 7496 = Live Trading
4. **Permissions** : Vérifier que le trading d'actions US est activé

**Paramètres ajustables :**
```python
bot = MMTradingBot(
    ib_host='127.0.0.1',
    ib_port=7497,              # Paper Trading
    divergence_threshold=10.0,  # Min divergence % pour trader
    scan_interval=300           # Intervalle entre scans (secondes)
)
```

## 🔍 Extraction Liste S&P 500 depuis IBKR

Le bot utilise **3 méthodes** pour obtenir la liste des actions à scanner :

### Méthode 1 : IBKR Scanner (Préféré)
```python
scanner = ScannerSubscription(
    instrument='STK',
    locationCode='STK.US.MAJOR',
    scanCode='TOP_PERC_GAIN'
)
scanner_data = await ib.reqScannerDataAsync(scanner)
```
**Avantage :** Garantit que toutes les actions sont tradeables sur votre compte IBKR

### Méthode 2 : Wikipedia + Validation IBKR (Fallback)
```python
# Récupère liste Wikipedia
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
tickers = pd.read_html(url)[0]['Symbol']

# Valide chaque ticker avec IBKR
for ticker in tickers:
    contract = Stock(ticker, 'SMART', 'USD')
    qualified = await ib.qualifyContractsAsync(contract)
```
**Avantage :** Liste officielle S&P 500, filtrée par disponibilité IBKR

### Méthode 3 : Liste Curated (Final Fallback)
Liste manuelle des 100+ principales entreprises US
**Avantage :** Fonctionne toujours, même sans internet

## 📊 Exemple de Résultats

### Screening Exemple (Décembre 2024)

```
TOP 10 OPPORTUNITES
============================================================
ticker signal  divergence_pct  confidence
    GS    BUY       58.888526  100.000000  ← Goldman Sachs très sous-évalué
   PFE    BUY       42.853155   85.706310  ← Pfizer sous-évalué
   TGT    BUY       36.109022   72.218043  ← Target sous-évalué
     C    BUY       25.370987   50.741974  ← Citigroup sous-évalué
   MCD    BUY       23.050128   46.100257  ← McDonald's sous-évalué
   UNH    BUY       17.168598   34.337197
   PEP    BUY       15.859460   31.718920
    KO    BUY       11.120933   22.241866
   BAC   HOLD        6.750621   50.000000
   CVX   HOLD        5.777461   50.000000
```

**Interprétation :**
- **GS (Goldman Sachs)** : Divergence de +58.9%
  - La valeur théorique ajustée suggère que l'action est sous-évaluée de 59%
  - Confidence maximale (100%) → Position sizing de 5% du capital
  - Signal : **STRONG BUY**

## ⚙️ Configuration Avancée

### Ajuster les Seuils

**Dans `MM_Trading_Bot.py` :**

```python
class DynamicPositionSizer:
    def __init__(self,
                 min_pct: float = 1.0,   # Position min (1% du capital)
                 max_pct: float = 5.0,   # Position max (5% du capital)
                 min_conf: float = 20.0  # Confidence min pour trader (20%)
    ):
```

**Exemples de configurations :**

**Conservative :**
```python
position_sizer = DynamicPositionSizer(min_pct=0.5, max_pct=2.0, min_conf=30.0)
```
- Positions : 0.5% - 2%
- Confidence min : 30%

**Aggressive :**
```python
position_sizer = DynamicPositionSizer(min_pct=2.0, max_pct=10.0, min_conf=15.0)
```
- Positions : 2% - 10%
- Confidence min : 15%

### Ajuster la Fréquence de Scan

```python
scan_interval=300   # 5 minutes (défaut)
scan_interval=60    # 1 minute (haute fréquence)
scan_interval=1800  # 30 minutes (basse fréquence)
```

### Limiter l'Univers de Trading

Pour scanner uniquement certains secteurs :

```python
# Uniquement finance
self.tickers_to_scan = ["JPM", "BAC", "GS", "WFC", "C", "MS", "BLK"]

# Uniquement tech
self.tickers_to_scan = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META"]

# Top 50 S&P 500 (plus liquides)
sp500 = await self.get_sp500_from_ibkr()
self.tickers_to_scan = sp500[:50]
```

## 📈 Performance & Risques

### Forces du Modèle

1. **Base théorique solide** : Nobel Prize (Modigliani & Miller 1985, 1990)
2. **Ajustements réalistes** : Intègre impôts, faillite, agence
3. **Objectif** : Ne dépend pas du sentiment de marché
4. **Quantifiable** : Chaque composante est mesurable

### Limitations

1. **Qualité des données** : Dépend de yfinance (peut avoir des erreurs)
2. **Hypothèses** :
   - Taux d'imposition stable
   - Coûts de détresse estimés (20% fixe)
   - Modèle de Merton simplifié
3. **Market timing** : Le modèle ne prédit pas QUAND la convergence aura lieu
4. **Frictions** : Ne considère pas les coûts de transaction

### Recommandations

1. **Backtest** : Tester sur données historiques avant live trading
2. **Diversification** : Ne pas concentrer sur 1-2 positions
3. **Stop-loss** : Ajouter des stop-loss (ex: -5% par position)
4. **Review** : Analyser manuellement les top opportunités avant trading
5. **Paper trading** : Commencer avec compte démo (port 7497)

## 🔧 Troubleshooting

### Problème : "Could not connect to TWS"

**Solutions :**
1. Vérifier que TWS est ouvert
2. File → Global Configuration → API → Enable ActiveX and Socket Clients
3. Vérifier le port (7497 = Paper, 7496 = Live)
4. Autoriser la connexion localhost dans TWS

### Problème : "No tickers loaded"

**Solutions :**
1. Vérifier la connexion internet (pour Wikipedia)
2. Vérifier les permissions IBKR (accès scanner API)
3. Utiliser la liste curated (fallback automatique)

### Problème : "Order failed"

**Solutions :**
1. Vérifier que le ticker est tradeable sur IBKR
2. Vérifier le capital disponible
3. Vérifier les permissions de trading (US Stocks)
4. Vérifier les heures de marché (9:30 - 16:00 EST)

## 📚 Références Académiques

1. **Modigliani, F., & Miller, M. H. (1958)**
   "The Cost of Capital, Corporation Finance and the Theory of Investment"
   *American Economic Review*, 48(3), 261-297

2. **Modigliani, F., & Miller, M. H. (1963)**
   "Corporate Income Taxes and the Cost of Capital: A Correction"
   *American Economic Review*, 53(3), 433-443

3. **Merton, R. C. (1974)**
   "On the Pricing of Corporate Debt: The Risk Structure of Interest Rates"
   *Journal of Finance*, 29(2), 449-470

4. **Warner, J. B. (1977)**
   "Bankruptcy Costs: Some Evidence"
   *Journal of Finance*, 32(2), 337-347

5. **Jensen, M. C., & Meckling, W. H. (1976)**
   "Theory of the Firm: Managerial Behavior, Agency Costs and Ownership Structure"
   *Journal of Financial Economics*, 3(4), 305-360

## 📞 Support

Pour des questions ou améliorations, consulter :
- Documentation IBKR API : https://interactivebrokers.github.io/tws-api/
- ib_insync documentation : https://ib-insync.readthedocs.io/

---

**Disclaimer :** Ce modèle est fourni à titre éducatif. Le trading comporte des risques. Toujours tester en paper trading avant de trader en réel.
