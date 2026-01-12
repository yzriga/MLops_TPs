# Systèmes pour le machine learning - CI6 : CI/CD pour systèmes ML + réentraînement automatisé + promotion MLflow
## Exercice 1: Mise en place du rapport et vérifications de départ
![alt text](../captures/image-71.png)
![alt text](../captures/image-72.png)
![alt text](../captures/image-73.png)
---
## Exercice 2: Ajouter une logique de décision testable (unit test)
![alt text](../captures/image-74.png)
```
On extrait une fonction pure pour tester la logique de décision indépendamment de Prefect/MLflow, ce qui rend les tests unitaires rapides, déterministes et faciles à débugger.
```
---
## Exercice 3: Créer le flow Prefect train_and_compare_flow (train → eval → compare → promote)
![alt text](../captures/image-75.png)
![alt text](../captures/image-76.png)
```
On utilise un delta pour éviter de promouvoir un modèle pour une amélioration trop faible (souvent due au hasard du split, au bruit ou à la variance du modèle). Cela agit comme un garde-fou : promotion seulement si le gain est significatif et reproductible.
```
---
## Exercice 4: Connecter drift → retraining automatique (monitor_flow.py)
![alt text](../captures/image-77.png)
![alt text](../captures/image-78.png)
---
## Exercice 5: Redémarrage API pour charger le nouveau modèle Production + test /predict
![alt text](../captures/image-79.png)
```
L’API doit être redémarrée car elle charge le modèle MLflow models:/streamflow_churn/Production au démarrage. Si une nouvelle version est promue en Production après coup, l’API ne la récupère pas automatiquement tant qu’on ne relance pas le processus (reload du modèle).
```
---
## Exercice 6: CI GitHub Actions (smoke + unit) avec Docker Compose
![alt text](../captures/image-80.png)
```
On démarre Docker Compose dans la CI pour réaliser un smoke test d’intégration multi-services : vérifier que l’API démarre correctement avec ses dépendances (Postgres, Feast, MLflow) et que l’endpoint /health répond. Cela détecte des problèmes de build, de réseau Docker, de variables d’environnement ou de dépendances entre conteneurs, que des tests unitaires seuls ne voient pas.
```
---
## Exercice 7: Synthèse finale : boucle complète drift → retrain → promotion → serving
### Question 7-a:
```
Dans ce projet, le drift est mesuré à l’aide d’Evidently en comparant deux périodes temporelles (month_000 vs month_001). Le rapport Evidently calcule notamment la part de features driftées (drift_share), c’est-à-dire la proportion de variables dont la distribution a significativement changé entre la période de référence et la période courante. Un seuil de 0.02 est utilisé pour déclencher le réentraînement automatique : ce seuil est volontairement bas dans le TP pour forcer le comportement, mais en pratique il serait plus élevé et ajusté selon le contexte métier et la sensibilité du modèle.

Lorsque le drift dépasse le seuil, le flow train_and_compare_flow est déclenché par Prefect. Ce flow entraîne un modèle candidat, l’évalue sur un jeu de validation (val_auc), puis évalue le modèle actuellement en Production sur exactement les mêmes données et le même split. La décision de promotion repose sur une règle simple et testable : le modèle candidat est promu uniquement si son AUC dépasse celle du modèle en Production d’au moins un delta (ex. 0.01). Cette logique évite les promotions inutiles dues au bruit statistique.

La responsabilité des outils est clairement séparée :

 - Prefect orchestre la logique métier ML (monitoring, entraînement, comparaison, promotion), avec une exécution conditionnelle basée sur les métriques.

 - GitHub Actions assure la qualité du code via la CI (tests unitaires et smoke tests Docker), sans jamais entraîner de modèle ni modifier l’état du système ML.
```
### Question 7-b:
```
La CI ne doit pas entraîner de modèle complet car l’entraînement est long, coûteux et non déterministe, et dépend de données et de services externes. La CI doit rester rapide et fiable, centrée sur la validation du code et de l’intégration des services.

Plusieurs tests manquent encore : tests de schéma de données, tests de cohérence des features Feast, tests de régression des performances, et tests de contrat API. En production, ces contrôles sont essentiels pour éviter des déploiements silencieusement dégradés.

Enfin, même avec de l’automatisation, une approbation humaine est souvent nécessaire. La promotion d’un modèle a des impacts business ou réglementaires, et doit souvent passer par des étapes de revue, de validation métier ou de gouvernance. Surtout quand on travaille sur des systèmes avec beaucoup de risques et dans lesquels la moindre erreur en production peut impacter les clients/utilisateurs.
```
